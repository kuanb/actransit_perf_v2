import sys
import unittest
from datetime import date
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import (
    aggregate_metric,
    comparable_route_group,
    default_analysis_window,
    resolve_daily_dates,
    route_group_for_id,
    route_ids_for_group,
    system_metric_row,
)


class ResolveDatesTest(unittest.TestCase):
    def test_defaults_to_seven_days_ending_yesterday_and_discloses_assumption(self):
        dates = [f"2026-10-{day:02d}" for day in range(1, 11)]
        resolved = resolve_daily_dates(dates, None, None, date(2026, 10, 11))

        self.assertEqual([f"2026-10-{day:02d}" for day in range(4, 11)], resolved.dates)
        self.assertEqual("2026-10-04", resolved.timeframe["start_date"])
        self.assertEqual("2026-10-10", resolved.timeframe["end_date"])
        self.assertTrue(resolved.timeframe["assumed"])
        self.assertIn("seven-day period ending yesterday", resolved.timeframe["disclosure"])

    def test_default_window_is_seven_calendar_days(self):
        self.assertEqual(
            ("2026-10-29", "2026-11-04"),
            default_analysis_window(date(2026, 11, 5)),
        )

    def test_explicit_range_uses_only_published_dates(self):
        resolved = resolve_daily_dates(["2026-10-01", "2026-10-03"], "2026-10-01", "2026-10-03")

        self.assertFalse(resolved.timeframe["assumed"])
        self.assertEqual(1, len(resolved.warnings))


class AggregateMetricTest(unittest.TestCase):
    def test_service_delivered_uses_counts_not_average_of_percentages(self):
        routes = [
            {"stop_sd_delivered_n": 1, "stop_sd_n": 2},
            {"stop_sd_delivered_n": 9, "stop_sd_n": 10},
        ]

        value, days = aggregate_metric(routes, "on_time_service_delivered")

        self.assertEqual(83.3, value)
        self.assertEqual(2, days)

    def test_dropped_trips_sum(self):
        routes = [
            {"scheduled_trips": 20, "ran_trips": 18},
            {"scheduled_trips": 10, "ran_trips": 7},
        ]

        value, _ = aggregate_metric(routes, "dropped_trips")

        self.assertEqual(5, value)

    def test_system_report_is_normalized_for_shared_metric_aggregation(self):
        row = system_metric_row(
            {
                "system": {"total_observations": 12, "p50_delay_minutes": 2.5},
                "schedule_compliance": {
                    "stop_sd_delivered_n": 8,
                    "stop_sd_n": 10,
                    "ran_trips": 9,
                    "scheduled_trips": 10,
                },
            }
        )

        self.assertEqual(12, row["observations"])
        self.assertEqual(8, row["stop_sd_delivered_n"])
        self.assertEqual(9, row["ran_trips"])


class RouteGroupTest(unittest.TestCase):
    def test_official_special_series_are_separate_from_regular_routes(self):
        expected = {
            "1T": "regular",
            "51A": "regular",
            "200": "regular",
            "633": "school",
            "802": "all_nighter",
            "NL": "transbay",
        }

        self.assertEqual(expected, {route: route_group_for_id(route) for route in expected})

    def test_cross_group_comparisons_are_rejected(self):
        self.assertEqual("regular", comparable_route_group(["1T", "51A"]))
        with self.assertRaises(ValueError):
            comparable_route_group(["1T", "802"])

    def test_default_peer_group_filter_excludes_special_service(self):
        route_ids = ["1T", "51A", "633", "802", "NL", "51A"]

        self.assertEqual(["1T", "51A"], route_ids_for_group(route_ids, "regular"))
        self.assertEqual(["633"], route_ids_for_group(route_ids, "school"))
        self.assertEqual(["802"], route_ids_for_group(route_ids, "all_nighter"))


if __name__ == "__main__":
    unittest.main()
