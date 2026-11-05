from __future__ import annotations

import asyncio
import hashlib
import math
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from google.cloud import firestore


PACIFIC = ZoneInfo("America/Los_Angeles")


class RateLimitError(Exception):
    def __init__(self, code: str, message: str, retry_after_seconds: int, active_queries: int) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retry_after_seconds = max(1, retry_after_seconds)
        self.active_queries = active_queries


@dataclass(frozen=True)
class LimitConfig:
    cooldown_seconds: int
    max_active_queries: int
    max_session_daily: int
    max_global_daily: int
    lease_seconds: int


@dataclass(frozen=True)
class Reservation:
    next_allowed_at: datetime
    active_queries: int


class FirestoreRateLimiter:
    def __init__(self, project_id: str, config: LimitConfig) -> None:
        self.client = firestore.Client(project=project_id)
        self.config = config
        self._local_lock = threading.Lock()
        self._local_next_allowed: dict[str, datetime] = {}
        self._global_exhausted_until: datetime | None = None
        self._status_lock = threading.Lock()
        self._status_cache_until = 0.0
        self._status_cache: dict[str, Any] | None = None

    async def status(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._status)

    async def reserve(self, session_id: str, request_id: str) -> Reservation:
        return await asyncio.to_thread(self._reserve, session_id, request_id)

    async def release(self, session_id: str, request_id: str) -> None:
        await asyncio.to_thread(self._release, session_id, request_id)

    async def get_cached(self, session_id: str, request_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_cached, session_id, request_id)

    async def store_result(self, session_id: str, request_id: str, result: dict[str, Any]) -> None:
        await asyncio.to_thread(self._store_result, session_id, request_id, result)

    def _status(self) -> dict[str, Any]:
        with self._status_lock:
            if self._status_cache and time.monotonic() < self._status_cache_until:
                return dict(self._status_cache)
            now = datetime.now(timezone.utc)
            day = now.astimezone(PACIFIC).date().isoformat()
            snapshot = self.client.collection("analysis_rate_limits").document(day).get()
            data = snapshot.to_dict() or {}
            active = prune_active(data.get("active") or {}, now)
            state = {
                "active_queries": len(active),
                "max_active_queries": self.config.max_active_queries,
                "cooldown_seconds": self.config.cooldown_seconds,
                "accepting": len(active) < self.config.max_active_queries
                and int(data.get("accepted") or 0) < self.config.max_global_daily,
                "global_questions_remaining_today": max(
                    0, self.config.max_global_daily - int(data.get("accepted") or 0)
                ),
            }
            self._status_cache = state
            self._status_cache_until = time.monotonic() + 5
            return dict(state)

    def _reserve(self, session_id: str, request_id: str) -> Reservation:
        now = datetime.now(timezone.utc)
        day = now.astimezone(PACIFIC).date().isoformat()
        session_key = digest(session_id)
        with self._local_lock:
            if self._global_exhausted_until and self._global_exhausted_until > now:
                raise RateLimitError(
                    "global_daily_limit",
                    "The public analysis budget has been used for today.",
                    math.ceil((self._global_exhausted_until - now).total_seconds()),
                    0,
                )
            local_next = self._local_next_allowed.get(session_key)
            if local_next and local_next > now:
                raise RateLimitError(
                    "cooldown",
                    "Please wait before asking another question.",
                    math.ceil((local_next - now).total_seconds()),
                    0,
                )
        global_ref = self.client.collection("analysis_rate_limits").document(day)
        session_ref = self.client.collection("analysis_sessions").document(session_key)
        transaction = self.client.transaction()

        @firestore.transactional
        def reserve_in_transaction(transaction):
            global_data = (global_ref.get(transaction=transaction).to_dict() or {})
            session_data = (session_ref.get(transaction=transaction).to_dict() or {})
            active = prune_active(global_data.get("active") or {}, now)
            accepted = int(global_data.get("accepted") or 0)

            if request_id in active:
                raise RateLimitError("in_progress", "This question is already being analyzed.", 2, len(active))
            if len(active) >= self.config.max_active_queries:
                soonest = min(as_datetime(expiry) for expiry in active.values())
                retry = max(2, math.ceil((soonest - now).total_seconds()))
                raise RateLimitError("capacity", "All analysis slots are currently in use.", retry, len(active))
            if accepted >= self.config.max_global_daily:
                tomorrow = datetime.combine(
                    now.astimezone(PACIFIC).date() + timedelta(days=1),
                    datetime.min.time(),
                    PACIFIC,
                ).astimezone(timezone.utc)
                raise RateLimitError(
                    "global_daily_limit",
                    "The public analysis budget has been used for today.",
                    math.ceil((tomorrow - now).total_seconds()),
                    len(active),
                )

            session_day = session_data.get("day")
            session_count = int(session_data.get("accepted") or 0) if session_day == day else 0
            if session_count >= self.config.max_session_daily:
                tomorrow = datetime.combine(
                    now.astimezone(PACIFIC).date() + timedelta(days=1),
                    datetime.min.time(),
                    PACIFIC,
                ).astimezone(timezone.utc)
                raise RateLimitError(
                    "session_daily_limit",
                    "This browser has used its daily analysis allowance.",
                    math.ceil((tomorrow - now).total_seconds()),
                    len(active),
                )
            active_expiry = session_data.get("active_expires_at")
            if active_expiry and as_datetime(active_expiry) > now:
                raise RateLimitError(
                    "session_active",
                    "This browser already has a question being analyzed.",
                    math.ceil((as_datetime(active_expiry) - now).total_seconds()),
                    len(active),
                )
            last_accepted = session_data.get("last_accepted_at")
            if last_accepted:
                next_allowed = as_datetime(last_accepted) + timedelta(seconds=self.config.cooldown_seconds)
                if next_allowed > now:
                    raise RateLimitError(
                        "cooldown",
                        "Please wait before asking another question.",
                        math.ceil((next_allowed - now).total_seconds()),
                        len(active),
                    )

            lease_expiry = now + timedelta(seconds=self.config.lease_seconds)
            active[request_id] = lease_expiry
            transaction.set(
                global_ref,
                {"day": day, "accepted": accepted + 1, "active": active, "updated_at": now},
            )
            transaction.set(
                session_ref,
                {
                    "day": day,
                    "accepted": session_count + 1,
                    "last_accepted_at": now,
                    "active_request_id": request_id,
                    "active_expires_at": lease_expiry,
                    "updated_at": now,
                },
            )
            return (
                Reservation(
                    next_allowed_at=now + timedelta(seconds=self.config.cooldown_seconds),
                    active_queries=len(active),
                ),
                self.config.max_global_daily - accepted - 1,
            )

        try:
            reservation, remaining = reserve_in_transaction(transaction)
        except RateLimitError as exc:
            with self._local_lock:
                if exc.code == "global_daily_limit":
                    self._global_exhausted_until = now + timedelta(seconds=exc.retry_after_seconds)
                elif exc.code in {"cooldown", "session_active", "session_daily_limit"}:
                    self._local_next_allowed[session_key] = now + timedelta(seconds=exc.retry_after_seconds)
            raise
        with self._local_lock:
            self._local_next_allowed[session_key] = reservation.next_allowed_at
            if remaining <= 0:
                tomorrow = datetime.combine(
                    now.astimezone(PACIFIC).date() + timedelta(days=1),
                    datetime.min.time(),
                    PACIFIC,
                ).astimezone(timezone.utc)
                self._global_exhausted_until = tomorrow
        self._invalidate_status()
        return reservation

    def _release(self, session_id: str, request_id: str) -> None:
        now = datetime.now(timezone.utc)
        day = now.astimezone(PACIFIC).date().isoformat()
        session_key = digest(session_id)
        global_ref = self.client.collection("analysis_rate_limits").document(day)
        session_ref = self.client.collection("analysis_sessions").document(session_key)
        transaction = self.client.transaction()

        @firestore.transactional
        def release_in_transaction(transaction):
            global_data = global_ref.get(transaction=transaction).to_dict() or {}
            session_data = session_ref.get(transaction=transaction).to_dict() or {}
            active = prune_active(global_data.get("active") or {}, now)
            active.pop(request_id, None)
            transaction.set(global_ref, {**global_data, "active": active, "updated_at": now})
            if session_data.get("active_request_id") == request_id:
                transaction.set(
                    session_ref,
                    {**session_data, "active_request_id": None, "active_expires_at": None, "updated_at": now},
                )

        release_in_transaction(transaction)
        self._invalidate_status()

    def _get_cached(self, session_id: str, request_id: str) -> dict[str, Any] | None:
        ref = self.client.collection("analysis_results").document(digest(f"{session_id}:{request_id}"))
        data = ref.get().to_dict() or {}
        if not data or as_datetime(data.get("expires_at")) <= datetime.now(timezone.utc):
            return None
        return data.get("response")

    def _store_result(self, session_id: str, request_id: str, result: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc)
        ref = self.client.collection("analysis_results").document(digest(f"{session_id}:{request_id}"))
        ref.set({"response": result, "created_at": now, "expires_at": now + timedelta(hours=1)})

    def _invalidate_status(self) -> None:
        with self._status_lock:
            self._status_cache_until = 0


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.min.replace(tzinfo=timezone.utc)


def prune_active(active: dict[str, Any], now: datetime) -> dict[str, datetime]:
    return {key: as_datetime(expiry) for key, expiry in active.items() if as_datetime(expiry) > now}
