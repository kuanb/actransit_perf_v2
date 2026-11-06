import json
import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from policy import (
    apply_context_timeframe,
    enforce_route_group,
    eval_prompt_features,
    extend_conversation,
    follow_ups_remaining,
    merge_tool_results,
    prepare_tool_call,
)


class ScopePolicyTest(unittest.TestCase):
    def test_route_scope_overwrites_model_route(self):
        arguments, _ = prepare_tool_call(
            "get_route_performance", {"route_id": "72"}, "51A", None
        )

        self.assertEqual("51A", arguments["route_id"])

    def test_route_scope_rejects_systemwide_ranking(self):
        with self.assertRaises(ValueError):
            prepare_tool_call("rank_routes", {"metric": "dropped_trips"}, "51A", None)

    def test_route_page_week_is_applied_and_disclosed(self):
        arguments, timeframe = prepare_tool_call(
            "get_route_performance", {"route_id": "51A"}, "51A", "2026-10-31"
        )

        self.assertEqual("2026-10-25", arguments["start_date"])
        result = apply_context_timeframe({"timeframe": {}}, timeframe)
        self.assertTrue(result["timeframe"]["assumed"])
        self.assertIn("week shown by the route page", result["timeframe"]["disclosure"])

    def test_open_ranking_defaults_to_regular_service(self):
        arguments, _ = prepare_tool_call(
            "rank_routes", {"metric": "median_delay"}, None, None
        )

        self.assertEqual("regular", arguments["route_group"])

    def test_eval_features_tag_category_sensitive_prompt_without_storing_it(self):
        features = eval_prompt_features(
            "Tell me about the worst hours of 1T performance this week? Any other popular routes that were worse?"
        )

        self.assertEqual(["ranking", "hourly", "popularity_language"], features)

    def test_popular_route_ranking_cannot_drift_into_special_service(self):
        arguments = enforce_route_group(
            "rank_routes",
            {"metric": "median_delay", "route_group": "all_nighter"},
            "Were any other popular routes worse?",
        )

        self.assertEqual("regular", arguments["route_group"])

    def test_explicit_school_request_selects_school_group(self):
        arguments = enforce_route_group(
            "rank_routes",
            {"metric": "median_delay", "route_group": "regular"},
            "Which 600-series school routes were worst?",
        )

        self.assertEqual("school", arguments["route_group"])


class MergeResultsTest(unittest.TestCase):
    def test_caps_charts_tables_and_chart_points(self):
        result = {
            "timeframe": {"start_date": "2026-10-01", "end_date": "2026-10-01"},
            "charts": [
                {
                    "type": "line",
                    "labels": list(range(300)),
                    "series": [{"name": "x", "values": list(range(300))}],
                }
            ],
            "tables": [{"rows": list(range(300))}],
            "sources": [{"kind": "daily"}],
            "warnings": [],
        }

        merged = merge_tool_results([result])

        self.assertEqual(200, len(merged["charts"][0]["labels"]))
        self.assertEqual(250, len(merged["tables"][0]["rows"]))

    def test_same_period_from_multiple_tools_produces_one_notice(self):
        first = {
            "timeframe": {
                "start_date": "2026-10-29",
                "end_date": "2026-11-04",
                "basis": "hourly",
                "disclosure": "Hourly disclosure",
            }
        }
        second = {
            "timeframe": {
                "start_date": "2026-10-29",
                "end_date": "2026-11-04",
                "basis": "ranking",
                "disclosure": "Ranking disclosure",
            }
        }

        merged = merge_tool_results([first, second])

        self.assertEqual(1, len(merged["timeframes"]))


class ConversationTest(unittest.TestCase):
    def test_initial_answer_allows_three_follow_ups(self):
        context = extend_conversation([], "How did Route 51A perform?", {"answer": "Well."}, 3)

        self.assertEqual(3, follow_ups_remaining(context, 3))

    def test_four_turn_thread_has_no_follow_ups_remaining(self):
        context = []
        for turn in range(4):
            context = extend_conversation(context, f"Question {turn}", {"answer": "Answer"}, 3)

        self.assertEqual(0, follow_ups_remaining(context, 3))
        with self.assertRaises(ValueError):
            extend_conversation(context, "One too many", {"answer": "No"}, 3)


class EvalCaseTest(unittest.TestCase):
    def test_route_category_eval_cases_have_unique_ids_and_expected_groups(self):
        path = Path(__file__).resolve().parents[1] / "evals" / "route_category_cases.json"
        cases = json.loads(path.read_text())

        self.assertEqual(len(cases), len({case["id"] for case in cases}))
        self.assertEqual(
            {"regular", "school", "all_nighter"},
            {
                group
                for case in cases
                for group in case.get("required_route_groups", [])
            },
        )


if __name__ == "__main__":
    unittest.main()
