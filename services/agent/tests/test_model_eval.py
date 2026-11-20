from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


EVAL_DIR = Path(__file__).resolve().parents[1] / "evals"
sys.path.insert(0, str(EVAL_DIR))

from model_eval import load_tool_catalog, parse_model_spec, score_turn


class ModelEvalTest(unittest.TestCase):
    def test_catalog_is_derived_from_mcp_tool_signatures(self) -> None:
        catalog = {item["name"]: item for item in load_tool_catalog()}
        self.assertEqual(
            set(catalog),
            {
                "compare_routes",
                "get_metric_definition",
                "get_route_delay_map",
                "get_route_performance",
                "get_route_segment_performance_map",
                "get_route_service_profile",
                "get_route_time_profile",
                "get_system_performance",
                "list_scheduled_arrivals",
                "rank_routes",
            },
        )
        rank_schema = catalog["rank_routes"]["input_schema"]
        self.assertEqual(rank_schema["properties"]["route_group"]["enum"], ["regular", "school", "all_nighter", "transbay"])
        self.assertEqual(rank_schema["properties"]["route_group"]["default"], "regular")
        self.assertEqual(rank_schema["required"], ["metric"])

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
            "answer_forbidden": ["delete from"],
            "required_artifacts": {"charts": ["bar", "heatmap"]},
            "max_calls": 0,
        }
        actual = {"answer": "I cannot do that.", "trace": [], "maps": [], "charts": [{"type": "bar"}, {"type": "heatmap"}]}
        self.assertTrue(score_turn(expected, actual)["passed"])
        actual["answer"] = "DELETE FROM observations"
        self.assertFalse(score_turn(expected, actual)["passed"])

    def test_model_spec_supports_per_model_location(self) -> None:
        spec = parse_model_spec("open:meta/llama-4-scout-17b-16e-instruct-maas@us-east5", "global")
        self.assertEqual(spec.location, "us-east5")
        self.assertEqual(spec.input_price, 0.25)
        self.assertEqual(spec.cached_input_price, 0.25)

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
