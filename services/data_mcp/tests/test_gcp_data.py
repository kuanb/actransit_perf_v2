import io
import sys
import unittest
import zipfile
from datetime import date
from pathlib import Path
from types import ModuleType
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import google.cloud.bigquery
except ModuleNotFoundError:
    google = ModuleType("google")
    google.__path__ = []
    cloud = ModuleType("google.cloud")
    cloud.__path__ = []
    bigquery = ModuleType("google.cloud.bigquery")
    storage = ModuleType("google.cloud.storage")
    cloud.bigquery = bigquery
    cloud.storage = storage
    google.cloud = cloud
    sys.modules.update(
        {
            "google": google,
            "google.cloud": cloud,
            "google.cloud.bigquery": bigquery,
            "google.cloud.storage": storage,
        }
    )

import gcp_data
from gcp_data import TransitData, gtfs_time_seconds, route_trip_templates


def gtfs_zip() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "WKD,1,1,1,1,1,1,1,20260101,20261231\n",
        )
        archive.writestr("calendar_dates.txt", "service_id,date,exception_type\n")
        archive.writestr(
            "trips.txt",
            "route_id,service_id,trip_id,direction_id\n"
            "1T,WKD,t1,0\n"
            "1T,WKD,t2,1\n"
            "72,WKD,other,0\n",
        )
        archive.writestr(
            "stop_times.txt",
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "t1,08:00:00,08:00:00,a,1\n"
            "t1,08:30:00,08:30:00,b,2\n"
            "t2,09:00:00,09:00:00,a,1\n"
            "t2,09:30:00,09:30:00,b,2\n"
            "other,10:00:00,10:00:00,a,1\n",
        )
    return buffer.getvalue()


class GTFSTemplateTest(unittest.TestCase):
    def test_route_templates_use_first_stop_and_filter_route(self):
        templates = route_trip_templates(gtfs_zip(), "1T")

        self.assertEqual(
            [
                {"trip_id": "t1", "start_seconds": 8 * 3600},
                {"trip_id": "t2", "start_seconds": 9 * 3600},
            ],
            templates["WKD"],
        )

    def test_gtfs_time_supports_after_midnight_service(self):
        self.assertEqual(25 * 3600 + 15 * 60, gtfs_time_seconds("25:15:00"))
        self.assertIsNone(gtfs_time_seconds("08:90:00"))


class RouteServiceProfileTest(unittest.TestCase):
    def test_returns_daily_absolute_bars_and_hour_of_week_heatmap(self):
        body = gtfs_zip()
        reports = {
            "2026-11-02": {
                "routes": [{"route_id": "1T", "scheduled_trips": 2, "ran_trips": 1}]
            },
            "2026-11-03": {
                "routes": [{"route_id": "1T", "scheduled_trips": 2, "ran_trips": 1}]
            },
        }
        transit = TransitData.__new__(TransitData)
        transit.project_id = "test-project"
        transit.published_dates = lambda: list(reports)
        transit.daily_reports = lambda dates: [reports[value] for value in dates]
        transit.gtfs_for_dates = lambda dates: {value: body for value in dates}
        transit.query_observed_trips = lambda route_id, start_date, end_date: [
            {"service_date": date(2026, 11, 2), "trip_id": "t1"},
            {"service_date": date(2026, 11, 3), "trip_id": "t2"},
        ]

        result = transit.get_route_service_profile("1T", "2026-11-02", "2026-11-03")

        self.assertEqual(["bar", "heatmap"], [chart["type"] for chart in result["charts"]])
        self.assertEqual([2, 2], result["charts"][0]["series"][0]["values"])
        self.assertEqual([1, 1], result["charts"][0]["series"][1]["values"])
        heatmap = result["charts"][1]
        self.assertEqual(168, len(heatmap["cells"]))
        monday_nine = next(
            cell for cell in heatmap["cells"] if cell["x"] == "09:00" and cell["y"] == "Monday"
        )
        self.assertEqual(
            {"x": "09:00", "y": "Monday", "value": 100.0, "scheduled": 1, "dropped": 1},
            monday_nine,
        )
        self.assertIn("2 of 4 scheduled trips", result["answer_context"])

    def test_observed_trip_query_splits_month_into_bounded_weekly_jobs(self):
        class FakeParameter:
            def __init__(self, name, _kind, value):
                self.name = name
                self.value = value

        class FakeConfig:
            def __init__(self, maximum_bytes_billed, query_parameters):
                self.maximum_bytes_billed = maximum_bytes_billed
                self.query_parameters = query_parameters

        class FakeJob:
            def result(self, timeout):
                self.timeout = timeout
                return []

        class FakeClient:
            def __init__(self):
                self.configs = []

            def query(self, _sql, job_config):
                self.configs.append(job_config)
                return FakeJob()

        transit = TransitData.__new__(TransitData)
        transit.project_id = "test-project"
        transit.maximum_bytes_billed = 100
        transit.bigquery = FakeClient()
        with (
            patch.object(gcp_data.bigquery, "ScalarQueryParameter", FakeParameter, create=True),
            patch.object(gcp_data.bigquery, "QueryJobConfig", FakeConfig, create=True),
        ):
            rows = transit.query_observed_trips("1T", "2026-10-06", "2026-11-05")

        self.assertEqual([], rows)
        self.assertEqual(5, len(transit.bigquery.configs))
        date_ranges = sorted(
            tuple(
                parameter.value
                for parameter in config.query_parameters
                if parameter.name in {"start_date", "end_date"}
            )
            for config in transit.bigquery.configs
        )
        self.assertEqual(("2026-10-06", "2026-10-12"), date_ranges[0])
        self.assertEqual(("2026-11-03", "2026-11-05"), date_ranges[-1])


if __name__ == "__main__":
    unittest.main()
