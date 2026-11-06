import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from policy import (
    apply_context_timeframe,
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


if __name__ == "__main__":
    unittest.main()
