import sys
import unittest
from datetime import date, timedelta
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import (
    aggregate_metric,
    build_trend_periods,
    calculate_metric_change,
    comparable_route_group,
    default_analysis_window,
    default_trend_window,
    resolve_daily_dates,
    route_group_for_id,
    route_ids_for_group,
    system_metric_row,
)


class ResolveDatesTest(unittest.TestCase):
    def test_defaults_to_four_weeks_ending_yesterday_and_discloses_assumption(self):
        dates = [
            (date(2026, 9, 13) + timedelta(days=offset)).isoformat()
            for offset in range(28)
        ]
        resolved = resolve_daily_dates(dates, None, None, date(2026, 10, 11))

        self.assertEqual(dates, resolved.dates)
        self.assertEqual("2026-09-13", resolved.timeframe["start_date"])
        self.assertEqual("2026-10-10", resolved.timeframe["end_date"])
        self.assertTrue(resolved.timeframe["assumed"])
        self.assertIn("four-week period ending yesterday", resolved.timeframe["disclosure"])

    def test_default_window_is_28_calendar_days(self):
        self.assertEqual(
            ("2026-10-08", "2026-11-04"),
            default_analysis_window(date(2026, 11, 5)),
        )

    def test_default_trend_window_is_28_calendar_days(self):
        self.assertEqual(
            ("2026-10-08", "2026-11-04"),
            default_trend_window(date(2026, 11, 5)),
        )

    def test_explicit_range_uses_only_published_dates(self):
        resolved = resolve_daily_dates(["2026-10-01", "2026-10-03"], "2026-10-01", "2026-10-03")

        self.assertFalse(resolved.timeframe["assumed"])
        self.assertEqual(1, len(resolved.warnings))


class TrendPeriodTest(unittest.TestCase):
    def test_week_periods_are_anchored_to_start_and_bound_the_last_period(self):
        periods = build_trend_periods("2026-10-12", "2026-11-08", "week")

        self.assertEqual(4, len(periods))
        self.assertEqual(("2026-10-12", "2026-10-18"), (periods[0].start_date, periods[0].end_date))
        self.assertEqual(("2026-11-02", "2026-11-08"), (periods[-1].start_date, periods[-1].end_date))

        partial = build_trend_periods("2026-10-12", "2026-10-20", "week")
        self.assertEqual(("2026-10-19", "2026-10-20"), (partial[-1].start_date, partial[-1].end_date))

    def test_day_periods_preserve_date_boundaries(self):
        periods = build_trend_periods("2026-11-01", "2026-11-03", "day")

        self.assertEqual(["2026-11-01", "2026-11-02", "2026-11-03"], [item.start_date for item in periods])

    def test_change_uses_first_to_last_and_omits_zero_baseline_percentage(self):
        worsened = calculate_metric_change(2, 10, "dropped_trips")
        zero_baseline = calculate_metric_change(0, 4, "dropped_trips")

        self.assertEqual(8, worsened["absolute_change"])
        self.assertEqual(400, worsened["percentage_change"])
        self.assertEqual("worsened", worsened["direction"])
        self.assertIsNone(zero_baseline["percentage_change"])


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
