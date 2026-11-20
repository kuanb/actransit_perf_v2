from __future__ import annotations

from typing import Any

from google import genai
from google.genai import types

from model_provider import ModelReply, ModelToolCall, ModelToolResult


class GeminiModelProvider:
    def __init__(self, project_id: str, location: str, model: str) -> None:
        self.client = genai.Client(enterprise=True, project=project_id, location=location)
        self.model = model

    def start_session(
        self,
        system_instruction: str,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]],
    ) -> "GeminiModelSession":
        declarations = [
            types.FunctionDeclaration(
                name=tool["name"],
                description=tool["description"],
                parameters_json_schema=tool["input_schema"],
            )
            for tool in tools
        ]
        contents = [
            types.Content(
                role="model" if message["role"] == "assistant" else "user",
                parts=[types.Part.from_text(text=message["content"])],
            )
            for message in messages
        ]
        return GeminiModelSession(
            self.client,
            self.model,
            system_instruction,
            contents,
            declarations,
        )


class GeminiModelSession:
    def __init__(
        self,
        client: genai.Client,
        model: str,
        system_instruction: str,
        contents: list[types.Content],
        declarations: list[types.FunctionDeclaration],
    ) -> None:
        self.client = client
        self.model = model
        self.system_instruction = system_instruction
        self.contents = contents
        self.config = types.GenerateContentConfig(
            system_instruction=system_instruction,
            tools=[types.Tool(function_declarations=declarations)],
            max_output_tokens=500,
            thinking_config=types.ThinkingConfig(thinking_level="MINIMAL"),
        )

    async def generate(self) -> ModelReply:
        response = await self.client.aio.models.generate_content(
            model=self.model,
            contents=self.contents,
            config=self.config,
        )
        return self._reply(response)

    async def submit_tool_results(
        self,
        reply: ModelReply,
        results: list[ModelToolResult],
    ) -> ModelReply:
        response = reply.provider_payload
        self.contents.append(response.candidates[0].content)
        self.contents.append(
            types.Content(
                role="user",
                parts=[
                    types.Part.from_function_response(
                        name=result.name,
                        response={"result": result.result},
                    )
                    for result in results
                ],
            )
        )
        return await self.generate()

    async def force_answer(self) -> ModelReply:
        response = await self.client.aio.models.generate_content(
            model=self.model,
            contents=self.contents,
            config=types.GenerateContentConfig(
                system_instruction=(
                    self.system_instruction
                    + "\nAnswer now using only the tool results already provided."
                ),
                max_output_tokens=500,
                thinking_config=types.ThinkingConfig(thinking_level="MINIMAL"),
            ),
        )
        return self._reply(response)

    @staticmethod
    def _reply(response: Any) -> ModelReply:
        calls = list(response.function_calls or [])
        metadata = response.usage_metadata
        return ModelReply(
            text=response.text or "",
            tool_calls=[
                ModelToolCall(
                    id=str(getattr(call, "id", None) or f"call_{index}"),
                    name=call.name,
                    arguments=dict(call.args or {}),
                )
                for index, call in enumerate(calls)
            ],
            provider_payload=response,
            usage={
                "input_tokens": int(getattr(metadata, "prompt_token_count", 0) or 0),
                "output_tokens": int(
                    (getattr(metadata, "candidates_token_count", 0) or 0)
                    + (getattr(metadata, "thoughts_token_count", 0) or 0)
                ),
                "cached_tokens": int(
                    getattr(metadata, "cached_content_token_count", 0) or 0
                ),
            },
        )
