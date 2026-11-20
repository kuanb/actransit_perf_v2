from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class ModelToolCall:
    id: str
    name: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class ModelToolResult:
    call_id: str
    name: str
    result: dict[str, Any]


@dataclass(frozen=True)
class ModelReply:
    text: str
    tool_calls: list[ModelToolCall]
    provider_payload: Any
    usage: dict[str, int] = field(
        default_factory=lambda: {
            "input_tokens": 0,
            "output_tokens": 0,
            "cached_tokens": 0,
        }
    )


class ModelSession(Protocol):
    async def generate(self) -> ModelReply: ...

    async def submit_tool_results(
        self,
        reply: ModelReply,
        results: list[ModelToolResult],
    ) -> ModelReply: ...

    async def force_answer(self) -> ModelReply: ...


class ModelProvider(Protocol):
    def start_session(
        self,
        system_instruction: str,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]],
    ) -> ModelSession: ...


def create_model_provider(
    provider: str,
    project_id: str,
    location: str,
    model: str,
) -> ModelProvider:
    if provider == "gemini":
        from gemini_model import GeminiModelProvider

        return GeminiModelProvider(project_id, location, model)
    if provider == "vertex_open":
        from vertex_open_model import VertexOpenModelProvider

        return VertexOpenModelProvider(project_id, location, model)
    raise ValueError(
        f"Unsupported analysis model provider {provider!r}; use 'gemini' or 'vertex_open'"
    )
