from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Callable


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
                "policy_version": "route-groups-v1",
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
