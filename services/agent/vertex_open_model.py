from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from model_provider import ModelReply, ModelToolCall, ModelToolResult


class VertexOpenModelProvider:
    def __init__(self, project_id: str, location: str, model: str) -> None:
        self.project_id = project_id
        self.location = location
        self.model = model
        self.timeout_seconds = float(
            os.environ.get("ANALYSIS_MODEL_TIMEOUT_SECONDS", "30")
        )

    def start_session(
        self,
        system_instruction: str,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]],
    ) -> "VertexOpenModelSession":
        from google import auth
        from google.auth.transport.requests import AuthorizedSession

        credentials, _ = auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        authorized_session = AuthorizedSession(credentials)
        hostname = (
            "aiplatform.googleapis.com"
            if self.location == "global"
            else f"{self.location}-aiplatform.googleapis.com"
        )
        url = (
            f"https://{hostname}/v1/projects/{self.project_id}/locations/"
            f"{self.location}/endpoints/openapi/chat/completions"
        )
        return VertexOpenModelSession(
            authorized_session,
            url,
            self.model,
            system_instruction,
            messages,
            tools,
            self.timeout_seconds,
        )


class VertexOpenModelSession:
    def __init__(
        self,
        authorized_session: Any,
        url: str,
        model: str,
        system_instruction: str,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]],
        timeout_seconds: float = 30,
    ) -> None:
        self.authorized_session = authorized_session
        self.url = url
        self.model = model
        self.system_instruction = system_instruction
        self.messages: list[dict[str, Any]] = [
            {"role": "system", "content": system_instruction},
            *[dict(message) for message in messages],
        ]
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": tool["name"],
                    "description": tool["description"],
                    "parameters": tool["input_schema"],
                },
            }
            for tool in tools
        ]
        self.timeout_seconds = timeout_seconds

    async def generate(self) -> ModelReply:
        body: dict[str, Any] = {
            "model": self.model,
            "messages": self.messages,
            "tools": self.tools,
            "tool_choice": "auto",
            "max_tokens": 500,
        }
        return await self._request(body)

    async def submit_tool_results(
        self,
        reply: ModelReply,
        results: list[ModelToolResult],
    ) -> ModelReply:
        self.messages.append(reply.provider_payload)
        self.messages.extend(
            {
                "role": "tool",
                "tool_call_id": result.call_id,
                "content": json.dumps(result.result, separators=(",", ":")),
            }
            for result in results
        )
        return await self.generate()

    async def force_answer(self) -> ModelReply:
        return await self._request(
            {
                "model": self.model,
                "messages": [
                    *self.messages,
                    {
                        "role": "user",
                        "content": "Answer now using only the tool results already provided.",
                    },
                ],
                "max_tokens": 500,
            }
        )

    async def _request(self, body: dict[str, Any]) -> ModelReply:
        response = await asyncio.to_thread(
            self.authorized_session.post,
            self.url,
            json=body,
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            raise RuntimeError(
                f"Vertex returned HTTP {response.status_code}: {response.text[:1000]}"
            )
        payload = response.json()
        choices = payload.get("choices") or []
        if not choices:
            raise RuntimeError(
                "Vertex response did not contain choices: "
                + json.dumps(payload, separators=(",", ":"))[:1000]
            )
        message = choices[0]["message"]
        calls = message.get("tool_calls") or []
        usage = payload.get("usage") or {}
        return ModelReply(
            text=message.get("content") or "",
            tool_calls=[self._tool_call(call, index) for index, call in enumerate(calls)],
            provider_payload=message,
            usage={
                "input_tokens": int(usage.get("prompt_tokens") or 0),
                "output_tokens": int(usage.get("completion_tokens") or 0),
                "cached_tokens": int(
                    (usage.get("prompt_tokens_details") or {}).get("cached_tokens")
                    or 0
                ),
            },
        )

    @staticmethod
    def _tool_call(call: dict[str, Any], index: int) -> ModelToolCall:
        function = call.get("function") or {}
        raw_arguments = function.get("arguments") or {}
        if isinstance(raw_arguments, dict):
            arguments = raw_arguments
        else:
            try:
                arguments = json.loads(raw_arguments)
            except (json.JSONDecodeError, TypeError):
                arguments = {}
        if not isinstance(arguments, dict):
            arguments = {}
        return ModelToolCall(
            id=str(call.get("id") or f"call_{index}"),
            name=str(function.get("name") or ""),
            arguments=arguments,
        )
