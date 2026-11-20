from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from model_provider import (
    ModelReply,
    ModelToolCall,
    ModelToolResult,
    create_model_provider,
)
from orchestrator import AnalysisAgent, tool_trace
from vertex_open_model import VertexOpenModelSession


class FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.ok = True
        self.status_code = 200
        self.text = json.dumps(payload)

    def json(self) -> dict[str, Any]:
        return self.payload


class FakeAuthorizedSession:
    def __init__(self, payloads: list[dict[str, Any]]) -> None:
        self.payloads = payloads
        self.requests: list[dict[str, Any]] = []

    def post(
        self,
        url: str,
        json: dict[str, Any],
        timeout: int,
    ) -> FakeResponse:
        self.requests.append({"url": url, "body": json, "timeout": timeout})
        return FakeResponse(self.payloads.pop(0))


class EvalTraceTest(unittest.TestCase):
    def test_route_trend_trace_keeps_period_granularity(self) -> None:
        trace = tool_trace(
            "rank_route_trends",
            {
                "metric": "dropped_trips",
                "period": "week",
                "order": "worsened",
            },
            "ok",
        )

        self.assertEqual("week", trace["arguments"]["period"])


class VertexOpenModelSessionTest(unittest.IsolatedAsyncioTestCase):
    async def test_translates_tools_calls_and_results(self) -> None:
        http = FakeAuthorizedSession(
            [
                {
                    "choices": [
                        {
                            "message": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call_1",
                                        "type": "function",
                                        "function": {
                                            "name": "get_route_performance",
                                            "arguments": '{"route_id":"51A"}',
                                        },
                                    }
                                ],
                            }
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 100,
                        "completion_tokens": 10,
                        "prompt_tokens_details": {"cached_tokens": 5},
                    },
                },
                {
                    "choices": [
                        {
                            "message": {
                                "role": "assistant",
                                "content": "Route 51A performed reliably.",
                            }
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 200,
                        "completion_tokens": 20,
                        "prompt_tokens_details": {"cached_tokens": 15},
                    },
                },
            ]
        )
        session = VertexOpenModelSession(
            http,
            "https://example.test/chat/completions",
            "google/gemma-test",
            "System instruction",
            [{"role": "user", "content": "How did 51A perform?"}],
            [
                {
                    "name": "get_route_performance",
                    "description": "Route summary",
                    "input_schema": {
                        "type": "object",
                        "properties": {"route_id": {"type": "string"}},
                    },
                }
            ],
        )

        reply = await session.generate()
        self.assertEqual(reply.tool_calls[0].name, "get_route_performance")
        self.assertEqual(reply.tool_calls[0].arguments, {"route_id": "51A"})
        self.assertEqual(
            reply.usage,
            {"input_tokens": 100, "output_tokens": 10, "cached_tokens": 5},
        )
        final = await session.submit_tool_results(
            reply,
            [
                ModelToolResult(
                    "call_1",
                    "get_route_performance",
                    {"answer_context": "Fixture result"},
                )
            ],
        )

        self.assertEqual(final.text, "Route 51A performed reliably.")
        first_body = http.requests[0]["body"]
        self.assertEqual(http.requests[0]["timeout"], 30)
        self.assertEqual(first_body["messages"][0], {"role": "system", "content": "System instruction"})
        self.assertEqual(first_body["tools"][0]["function"]["name"], "get_route_performance")
        second_messages = http.requests[1]["body"]["messages"]
        self.assertEqual(second_messages[-1]["tool_call_id"], "call_1")
        self.assertEqual(
            json.loads(second_messages[-1]["content"]),
            {"answer_context": "Fixture result"},
        )

    async def test_reports_success_response_without_choices(self) -> None:
        session = VertexOpenModelSession(
            FakeAuthorizedSession([{"error": {"message": "capacity unavailable"}}]),
            "https://example.test/chat/completions",
            "model",
            "System",
            [],
            [],
        )

        with self.assertRaisesRegex(RuntimeError, "capacity unavailable"):
            await session.generate()


class FakeModelSession:
    def __init__(self) -> None:
        self.results: list[ModelToolResult] = []

    async def generate(self) -> ModelReply:
        return ModelReply(
            "",
            [
                ModelToolCall(
                    "call_1",
                    "get_route_performance",
                    {"route_id": "72"},
                )
            ],
            {},
            {"input_tokens": 100, "output_tokens": 10, "cached_tokens": 5},
        )

    async def submit_tool_results(
        self,
        reply: ModelReply,
        results: list[ModelToolResult],
    ) -> ModelReply:
        self.results = results
        return ModelReply(
            "Route 51A was reliable.",
            [],
            {},
            {"input_tokens": 200, "output_tokens": 20, "cached_tokens": 15},
        )

    async def force_answer(self) -> ModelReply:
        raise AssertionError("force_answer should not be called")


class FakeModelProvider:
    def __init__(self) -> None:
        self.session = FakeModelSession()
        self.tools: list[dict[str, Any]] = []

    def start_session(
        self,
        system_instruction: str,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]],
    ) -> FakeModelSession:
        self.tools = tools
        return self.session


class FakeGateway:
    def __init__(self) -> None:
        self.arguments: dict[str, Any] = {}

    async def list_tools(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "get_route_performance",
                "description": "Route summary",
                "input_schema": {"type": "object"},
            },
            {
                "name": "get_system_performance",
                "description": "System summary",
                "input_schema": {"type": "object"},
            },
        ]

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        self.arguments = arguments
        return {
            "answer_context": "Fixture result",
            "timeframe": {},
            "maps": [],
            "charts": [],
            "tables": [{"rows": [{"route_id": "51A"}]}],
            "sources": [],
            "warnings": [],
        }


class AnalysisAgentProviderTest(unittest.IsolatedAsyncioTestCase):
    async def test_shared_policy_loop_runs_through_provider_session(self) -> None:
        provider = FakeModelProvider()
        gateway = FakeGateway()
        agent = AnalysisAgent(provider, gateway, 4)

        result = await agent.analyze("How did it perform?", "51A", None, [])

        self.assertEqual(result["answer"], "Route 51A was reliable.")
        self.assertEqual(gateway.arguments["route_id"], "51A")
        self.assertEqual([tool["name"] for tool in provider.tools], ["get_route_performance"])
        self.assertEqual(provider.session.results[0].call_id, "call_1")
        self.assertEqual(result["tables"][0]["rows"][0]["route_id"], "51A")
        self.assertEqual(
            result["_model_usage"],
            {"input_tokens": 300, "output_tokens": 30, "cached_tokens": 20},
        )


class ModelProviderFactoryTest(unittest.TestCase):
    def test_constructs_vertex_open_provider_without_authentication(self) -> None:
        provider = create_model_provider(
            "vertex_open",
            "project",
            "global",
            "google/gemma-test",
        )

        self.assertEqual(type(provider).__name__, "VertexOpenModelProvider")

    def test_constructs_vertex_mistral_provider_without_authentication(self) -> None:
        provider = create_model_provider(
            "vertex_mistral",
            "project",
            "us-central1",
            "mistral-small-2503",
        )

        self.assertEqual(type(provider).__name__, "VertexMistralModelProvider")

    def test_vertex_mistral_uses_regional_raw_predict_endpoint(self) -> None:
        provider = create_model_provider(
            "vertex_mistral",
            "project",
            "us-central1",
            "mistral-small-2503",
        )
        self.assertEqual(
            provider.endpoint_url(),
            "https://us-central1-aiplatform.googleapis.com/v1/projects/project/"
            "locations/us-central1/publishers/mistralai/models/"
            "mistral-small-2503:rawPredict",
        )

    def test_vertex_open_timeout_can_be_overridden_for_evals(self) -> None:
        with patch.dict(os.environ, {"ANALYSIS_MODEL_TIMEOUT_SECONDS": "90"}):
            provider = create_model_provider(
                "vertex_open",
                "project",
                "global",
                "google/gemma-test",
            )

        self.assertEqual(provider.timeout_seconds, 90)

    def test_rejects_unknown_provider(self) -> None:
        with self.assertRaisesRegex(ValueError, "gemini.*vertex_open.*vertex_mistral"):
            create_model_provider("other", "project", "global", "model")


if __name__ == "__main__":
    unittest.main()
