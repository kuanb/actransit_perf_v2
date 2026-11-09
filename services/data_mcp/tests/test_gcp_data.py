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


def processed_route() -> dict:
    return {
        "route_id": "1T",
        "feed_hash": "feed-123",
        "shapes": {
            "outbound": [
                [37.80, -122.30, 0.0],
                [37.81, -122.29, 1000.0],
                [37.82, -122.28, 2000.0],
            ],
            "inbound": [
                [37.82, -122.28, 0.0],
                [37.81, -122.29, 1000.0],
                [37.80, -122.30, 2000.0],
            ],
        },
        "stops": {
            "a": {"stop_id": "a", "stop_name": "First", "lat": 37.80, "lon": -122.30},
            "b": {"stop_id": "b", "stop_name": "Second", "lat": 37.82, "lon": -122.28},
        },
        "trips": {
            "t1": {
                "trip_id": "t1",
                "shape_id": "outbound",
                "direction_id": 0,
                "stop_times": [
                    {"stop_sequence": 1, "stop_id": "a", "dist_along_route_m": 0.0},
                    {"stop_sequence": 2, "stop_id": "b", "dist_along_route_m": 2000.0},
                ],
            },
            "t2": {
                "trip_id": "t2",
                "shape_id": "inbound",
                "direction_id": 1,
                "stop_times": [
                    {"stop_sequence": 1, "stop_id": "b", "dist_along_route_m": 0.0},
                    {"stop_sequence": 2, "stop_id": "a", "dist_along_route_m": 2000.0},
                ],
            },
        },
    }


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


class RouteMapTest(unittest.TestCase):
    def transit(self):
        transit = TransitData.__new__(TransitData)
        transit.project_id = "test-project"
        transit.published_dates = lambda: [
            "2026-11-02",
            "2026-11-03",
            "2026-11-04",
            "2026-11-05",
            "2026-11-06",
            "2026-11-07",
            "2026-11-08",
        ]
        transit.read_json = lambda _key: processed_route()
        return transit

    def test_delay_map_returns_one_styled_geojson_per_direction(self):
        transit = self.transit()
        transit.query_stop_map_stats = lambda *_args: [
            {
                "direction_id": 0,
                "stop_id": "a",
                "observations": 20,
                "p50_delay_s": 180,
                "p95_delay_s": 480,
                "stddev_delay_s": 120,
            },
            {
                "direction_id": 1,
                "stop_id": "b",
                "observations": 18,
                "p50_delay_s": 60,
                "p95_delay_s": 300,
                "stddev_delay_s": 90,
            },
        ]

        result = transit.get_route_delay_map(
            "1T",
            "p95_delay",
            "2026-11-02",
            "2026-11-08",
        )

        self.assertEqual(2, len(result["maps"]))
        self.assertEqual("stop", result["maps"][0]["style"]["target"])
        features = result["maps"][0]["geojson"]["features"]
        self.assertTrue(any(feature["properties"]["feature_kind"] == "route" for feature in features))
        self.assertTrue(any(feature["properties"]["feature_kind"] == "stop" for feature in features))
        self.assertIn("8.0 min", result["answer_context"])

    def test_segment_map_builds_route_slices_and_stop_landmarks(self):
        transit = self.transit()
        transit.query_segment_map_stats = lambda *_args: [
            {
                "direction_id": 0,
                "from_stop_id": "a",
                "to_stop_id": "b",
                "observations": 15,
                "p50_schedule_loss_s": 90,
                "p95_schedule_loss_s": 240,
                "p50_travel_time_s": 360,
                "p95_travel_time_s": 600,
                "p50_speed_mps": 4.0,
            }
        ]

        result = transit.get_route_segment_performance_map(
            "1T",
            "median_schedule_loss",
            "2026-11-02",
            "2026-11-08",
        )

        self.assertEqual(1, len(result["maps"]))
        map_result = result["maps"][0]
        self.assertEqual("segment", map_result["style"]["target"])
        segment = next(
            feature
            for feature in map_result["geojson"]["features"]
            if feature["properties"]["feature_kind"] == "segment"
        )
        self.assertGreaterEqual(len(segment["geometry"]["coordinates"]), 2)
        self.assertEqual(1.5, segment["properties"]["metric_value"])

    def test_maps_reject_more_than_seven_calendar_days(self):
        transit = self.transit()
        transit.published_dates = lambda: ["2026-11-01", "2026-11-08"]

        with self.assertRaisesRegex(ValueError, "seven calendar days"):
            transit.get_route_delay_map("1T", start_date="2026-11-01", end_date="2026-11-08")

    def test_spatial_query_uses_only_selected_published_dates(self):
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
                self.config = None

            def query(self, _sql, job_config):
                self.config = job_config
                return FakeJob()

        transit = TransitData.__new__(TransitData)
        transit.maximum_bytes_billed = 100
        transit.bigquery = FakeClient()
        with (
            patch.object(gcp_data.bigquery, "ArrayQueryParameter", FakeParameter, create=True),
            patch.object(gcp_data.bigquery, "ScalarQueryParameter", FakeParameter, create=True),
            patch.object(gcp_data.bigquery, "QueryJobConfig", FakeConfig, create=True),
        ):
            transit.run_spatial_query(
                "SELECT 1",
                "1T",
                ["2026-11-02", "2026-11-04"],
                None,
            )

        dates = next(
            parameter.value
            for parameter in transit.bigquery.config.query_parameters
            if parameter.name == "service_dates"
        )
        self.assertEqual([date(2026, 11, 2), date(2026, 11, 4)], dates)

    def test_segment_query_reads_only_the_requested_metric(self):
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
                return []

        class FakeClient:
            def __init__(self):
                self.sql = ""

            def query(self, sql, job_config):
                self.sql = sql
                return FakeJob()

        transit = TransitData.__new__(TransitData)
        transit.project_id = "transit-203605"
        transit.maximum_bytes_billed = 100
        transit.bigquery = FakeClient()
        with (
            patch.object(gcp_data.bigquery, "ArrayQueryParameter", FakeParameter, create=True),
            patch.object(gcp_data.bigquery, "ScalarQueryParameter", FakeParameter, create=True),
            patch.object(gcp_data.bigquery, "QueryJobConfig", FakeConfig, create=True),
        ):
            transit.query_segment_map_stats("1T", ["2026-11-08"], None, "median_schedule_loss")
            self.assertIn("delay_seconds AS metric_source", transit.bigquery.sql)
            self.assertNotIn("scheduled_arrival", transit.bigquery.sql)
            self.assertNotIn("leg_duration_s", transit.bigquery.sql)

            transit.query_segment_map_stats("1T", ["2026-11-08"], None, "median_travel_time")
            self.assertIn("leg_duration_s AS metric_source", transit.bigquery.sql)
            self.assertNotIn("delay_seconds", transit.bigquery.sql)
            self.assertNotIn("leg_avg_speed_mps", transit.bigquery.sql)


if __name__ == "__main__":
    unittest.main()
