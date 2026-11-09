from __future__ import annotations

import asyncio
import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Callable


PUBLIC_CHAT_PREFIX = "analysis/public-chats"
PUBLIC_RESPONSE_FIELDS = (
    "answer",
    "timeframes",
    "warnings",
    "maps",
    "charts",
    "tables",
    "sources",
)
PUBLIC_TEXT_REDACTIONS = (
    (
        re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE),
        "[email removed]",
    ),
    (re.compile(r"https?://\S+", re.IGNORECASE), "[link removed]"),
    (
        re.compile(r"(?<!\d)(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]\d{4}(?!\d)"),
        "[phone number removed]",
    ),
    (re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"), "[IP address removed]"),
)


class ConversationRecorder:
    def __init__(
        self,
        bucket_name: str,
        client: Any | None = None,
        clock: Callable[[], datetime] | None = None,
    ):
        if not bucket_name:
            raise ValueError("conversation log bucket is required")
        if client is None:
            from google.cloud import storage

            client = storage.Client()
        self.bucket = client.bucket(bucket_name)
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    async def record(
        self,
        *,
        request_id: str,
        parent_request_id: str | None,
        thread_id: str,
        turn_index: int,
        session_id: str,
        question: str,
        response: dict[str, Any],
        scope: dict[str, Any],
        model: str,
        status: str,
        latency_ms: int,
        prompt_features: list[str],
        tool_calls: list[dict[str, Any]],
    ) -> str:
        return await asyncio.to_thread(
            self._record,
            request_id=request_id,
            parent_request_id=parent_request_id,
            thread_id=thread_id,
            turn_index=turn_index,
            session_id=session_id,
            question=question,
            response=response,
            scope=scope,
            model=model,
            status=status,
            latency_ms=latency_ms,
            prompt_features=prompt_features,
            tool_calls=tool_calls,
        )

    def _record(self, **values: Any) -> str:
        recorded_at = self.clock().astimezone(timezone.utc)
        request_id = values["request_id"]
        session_hash = hashlib.sha256(values["session_id"].encode()).hexdigest()
        record_key = hashlib.sha256(
            f"{values['session_id']}:{request_id}".encode()
        ).hexdigest()[:16]
        object_name = (
            f"date={recorded_at:%Y-%m-%d}/hour={recorded_at:%H}/"
            f"{recorded_at:%Y%m%dT%H%M%S.%fZ}-{record_key}.json"
        )
        payload = {
            "schema_version": 1,
            "recorded_at": recorded_at.isoformat(),
            "status": values["status"],
            "request_id": request_id,
            "parent_request_id": values["parent_request_id"],
            "thread_id": values["thread_id"],
            "turn_index": values["turn_index"],
            "session_hash": session_hash,
            "scope": values["scope"],
            "question": values["question"],
            "response": values["response"],
            "evaluation": {
                "policy_version": "spatial-map-contract-v3",
                "prompt_features": values["prompt_features"],
                "tool_calls": values["tool_calls"],
            },
            "runtime": {
                "model": values["model"],
                "latency_ms": values["latency_ms"],
            },
        }
        self.bucket.blob(object_name).upload_from_string(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str),
            content_type="application/json",
            if_generation_match=0,
        )
        return object_name


class PublicConversationPublisher:
    def __init__(
        self,
        bucket_name: str,
        client: Any | None = None,
        clock: Callable[[], datetime] | None = None,
    ):
        if not bucket_name:
            raise ValueError("public conversation bucket is required")
        if client is None:
            from google.cloud import storage

            client = storage.Client()
        self.bucket = client.bucket(bucket_name)
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    async def publish(
        self,
        *,
        thread_id: str,
        turn_index: int,
        question: str,
        response: dict[str, Any],
        scope: dict[str, Any],
    ) -> str:
        return await asyncio.to_thread(
            self._publish,
            thread_id=thread_id,
            turn_index=turn_index,
            question=question,
            response=response,
            scope=scope,
        )

    def _publish(self, **values: Any) -> str:
        published_at = self.clock().astimezone(timezone.utc)
        conversation_id = hashlib.sha256(values["thread_id"].encode()).hexdigest()[:16]
        object_name = (
            f"{PUBLIC_CHAT_PREFIX}/date={published_at:%Y-%m-%d}/hour={published_at:%H}/"
            f"{published_at:%Y%m%dT%H%M%S.%fZ}-{conversation_id}-turn-{values['turn_index']:02d}.json"
        )
        public_response = {
            field: redact_public_value(values["response"][field])
            for field in PUBLIC_RESPONSE_FIELDS
            if field in values["response"]
        }
        payload = {
            "schema_version": 1,
            "published_at": published_at.isoformat(),
            "conversation_id": conversation_id,
            "turn_index": values["turn_index"],
            "scope": {
                "route_id": values["scope"].get("route_id"),
                "week_end": values["scope"].get("week_end"),
            },
            "question": redact_public_text(values["question"]),
            "response": public_response,
        }
        blob = self.bucket.blob(object_name)
        blob.cache_control = "public, max-age=31536000, immutable"
        blob.upload_from_string(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str),
            content_type="application/json",
            if_generation_match=0,
        )
        return object_name


def redact_public_text(value: str) -> str:
    redacted = value
    for pattern, replacement in PUBLIC_TEXT_REDACTIONS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


def redact_public_value(value: Any) -> Any:
    if isinstance(value, str):
        return redact_public_text(value)
    if isinstance(value, list):
        return [redact_public_value(item) for item in value]
    if isinstance(value, dict):
        return {key: redact_public_value(item) for key, item in value.items()}
    return value
