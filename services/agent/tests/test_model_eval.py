from __future__ import annotations

import asyncio
import json
import sys
import unittest
from pathlib import Path


EVAL_DIR = Path(__file__).resolve().parents[1] / "evals"
sys.path.insert(0, str(EVAL_DIR))

from model_eval import ManagedModelRunner, ModelSpec, load_tool_catalog, parse_model_spec, score_turn, summarize


class LoopBoundAgent:
    def __init__(self) -> None:
        self.loop = None

    async def analyze(self, prompt, route_id, week_end, conversation):
        loop = asyncio.get_running_loop()
        if self.loop is None:
            self.loop = loop
        elif self.loop is not loop:
            raise RuntimeError("event loop changed")
        return {
            "answer": prompt,
            "_eval_trace": [],
            "_model_usage": {
                "input_tokens": 1,
                "output_tokens": 1,
                "cached_tokens": 0,
            },
        }


class ModelEvalTest(unittest.TestCase):
    def test_runner_reuses_one_event_loop_across_turns(self) -> None:
        runner = ManagedModelRunner.__new__(ManagedModelRunner)
        runner.agent = LoopBoundAgent()
        runner.loop = asyncio.new_event_loop()
        try:
            self.assertEqual(runner.run_turn("first", {}, [])["answer"], "first")
            self.assertEqual(runner.run_turn("second", {}, [])["answer"], "second")
        finally:
            runner.close()

    def test_catalog_is_derived_from_mcp_tool_signatures(self) -> None:
        catalog = {item["name"]: item for item in load_tool_catalog()}
        self.assertEqual(
            set(catalog),
            {
                "compare_routes",
                "describe_data",
                "get_metric_definition",
                "get_route_delay_map",
                "get_route_performance",
                "get_route_segment_performance_map",
                "get_route_service_profile",
                "get_route_time_profile",
                "get_system_performance",
                "list_scheduled_arrivals",
                "query_data",
                "rank_route_trends",
                "rank_routes",
            },
        )
        rank_schema = catalog["rank_routes"]["input_schema"]
        self.assertEqual(rank_schema["properties"]["route_group"]["enum"], ["regular", "school", "all_nighter", "transbay"])
        self.assertEqual(rank_schema["properties"]["route_group"]["default"], "regular")
        self.assertEqual(rank_schema["required"], ["metric"])
        trend_schema = catalog["rank_route_trends"]["input_schema"]
        self.assertEqual(trend_schema["properties"]["metric"]["enum"], ["dropped_trips"])
        self.assertEqual(trend_schema["properties"]["period"]["default"], "week")
        self.assertEqual(trend_schema["properties"]["order"]["default"], "worsened")
        query_schema = catalog["query_data"]["input_schema"]
        aggregation_schema = query_schema["properties"]["aggregations"]["items"]
        self.assertEqual(
            aggregation_schema["properties"]["operation"]["enum"],
            ["count", "count_distinct", "sum", "avg", "min", "max", "median", "p95", "stddev"],
        )
        self.assertEqual(
            aggregation_schema["required"],
            ["operation", "alias"],
        )

    def test_score_requires_exact_tool_and_expected_argument_subset(self) -> None:
        expected = {
            "expected_calls": [{"name": "get_route_performance", "arguments": {"route_id": "1T"}}],
            "max_calls": 1,
        }
        actual = {
            "answer": "Fixture",
            "trace": [{"tool": "get_route_performance", "arguments": {"route_id": "1T", "start_date": "2026-11-01"}, "status": "ok"}],
            "maps": [],
            "charts": [],
        }
        score = score_turn(expected, actual)
        self.assertTrue(score["passed"])
        actual["trace"][0]["arguments"]["route_id"] = "51B"
        self.assertFalse(score_turn(expected, actual)["passed"])

    def test_score_checks_artifacts_and_answer_policy(self) -> None:
        expected = {
            "expected_calls": [],
            "answer_contains_any": ["cannot"],
            "answer_contains_all": ["cannot", "that"],
            "answer_forbidden": ["delete from"],
            "required_artifacts": {"charts": ["bar", "heatmap"], "chart_count": 2, "tables": 1},
            "max_calls": 0,
        }
        actual = {
            "answer": "I cannot do that.",
            "trace": [],
            "maps": [],
            "charts": [{"type": "bar"}, {"type": "heatmap"}],
            "tables": [{}],
        }
        self.assertTrue(score_turn(expected, actual)["passed"])
        actual["tables"] = []
        self.assertFalse(score_turn(expected, actual)["passed"])
        actual["tables"] = [{}]
        actual["answer"] = "DELETE FROM observations"
        self.assertFalse(score_turn(expected, actual)["passed"])

    def test_score_requires_every_numeric_grounding_token(self) -> None:
        expected = {
            "expected_calls": [],
            "answer_contains_all": ["1T", "28 trips", "51A", "from 2", "to 10"],
            "max_calls": 0,
        }
        actual = {
            "answer": "Route 1T had 28 trips. Route 51A worsened from 2 to 10.",
            "trace": [],
            "maps": [],
            "charts": [],
        }

        self.assertTrue(score_turn(expected, actual)["passed"])
        actual["answer"] = "Route 1T had 28 trips. Route 51A worsened."
        self.assertFalse(score_turn(expected, actual)["passed"])

    def test_score_checks_resolved_timeframe(self) -> None:
        expected = {
            "expected_calls": [],
            "expected_timeframe": {"start_date": "2026-10-12", "end_date": "2026-11-08"},
            "max_calls": 0,
        }
        actual = {
            "answer": "Fixture",
            "trace": [],
            "maps": [],
            "charts": [],
            "tables": [],
            "timeframes": [{"start_date": "2026-10-12", "end_date": "2026-11-08"}],
        }

        self.assertTrue(score_turn(expected, actual)["passed"])
        actual["timeframes"][0]["start_date"] = "2026-10-13"
        self.assertFalse(score_turn(expected, actual)["passed"])

    def test_model_spec_supports_per_model_location(self) -> None:
        spec = parse_model_spec("open:meta/llama-4-scout-17b-16e-instruct-maas@us-east5", "global")
        self.assertEqual(spec.location, "us-east5")
        self.assertEqual(spec.input_price, 0.25)
        self.assertEqual(spec.cached_input_price, 0.25)

    def test_model_spec_supports_mistral_endpoint(self) -> None:
        spec = parse_model_spec("mistral:mistral-small-2503@us-central1", "global")
        self.assertEqual(spec.provider, "mistral")
        self.assertEqual(spec.location, "us-central1")
        self.assertEqual(spec.output_price, 0.30)

    def test_summary_excludes_provider_failures_from_quality_and_latency(self) -> None:
        spec = ModelSpec("open", "test/model", "global", 1.0, 2.0, 0.5)
        completed = {
            "scores": {
                "passed": True,
                "score": 1.0,
                "tool_selection": 1.0,
                "arguments": 1.0,
                "answer_policy": 1.0,
                "artifacts": 1.0,
            },
            "usage": {"input_tokens": 100, "output_tokens": 50, "cached_tokens": 20},
            "latency_seconds": 2.0,
            "critical": True,
            "error": None,
        }
        failed = {
            "scores": {
                "passed": False,
                "score": 0.0,
                "tool_selection": 0.0,
                "arguments": 0.0,
                "answer_policy": 0.0,
                "artifacts": 0.0,
            },
            "usage": {"input_tokens": 0, "output_tokens": 0, "cached_tokens": 0},
            "latency_seconds": 30.0,
            "critical": True,
            "error": "model unavailable",
        }

        summary = summarize(spec, [completed, failed])

        self.assertEqual(summary["completed_runs"], 1)
        self.assertEqual(summary["errors"], 1)
        self.assertEqual(summary["pass_rate"], 1.0)
        self.assertEqual(summary["critical_pass_rate"], 1.0)
        self.assertEqual(summary["mean_latency_seconds"], 2.0)
        self.assertEqual(summary["estimated_cost_per_turn_usd"], 0.00019)

    def test_cases_only_expect_current_mcp_tools_and_arguments(self) -> None:
        catalog = {item["name"]: item for item in load_tool_catalog()}
        suite = json.loads((EVAL_DIR / "model_comparison_cases.json").read_text())
        self.assertGreaterEqual(len(suite["cases"]), 15)
        for case in suite["cases"]:
            for turn in case["turns"]:
                for call in turn.get("expected_calls") or []:
                    self.assertIn(call["name"], catalog, case["id"])
                    properties = catalog[call["name"]]["input_schema"]["properties"]
                    self.assertTrue(set(call.get("arguments") or {}).issubset(properties), case["id"])


if __name__ == "__main__":
    unittest.main()
