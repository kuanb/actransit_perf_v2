import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import aggregate_metric, resolve_daily_dates, system_metric_row


class ResolveDatesTest(unittest.TestCase):
    def test_defaults_to_latest_published_service_days_and_discloses_assumption(self):
        dates = [f"2026-10-{day:02d}" for day in range(1, 11)]
        resolved = resolve_daily_dates(dates, None, None, 3, "the latest three published service days")

        self.assertEqual(["2026-10-08", "2026-10-09", "2026-10-10"], resolved.dates)
        self.assertTrue(resolved.timeframe["assumed"])
        self.assertIn("No timeframe was provided", resolved.timeframe["disclosure"])

    def test_explicit_range_uses_only_published_dates(self):
        resolved = resolve_daily_dates(
            ["2026-10-01", "2026-10-03"], "2026-10-01", "2026-10-03", 1, "latest"
        )

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


if __name__ == "__main__":
    unittest.main()
