import json
import sys
import unittest
from datetime import date
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from structured_query import (
    TABLES,
    build_artifacts,
    compile_query,
    describe_schema,
    resolve_query_dates,
)


class StructuredQueryCompilerTest(unittest.TestCase):
    def test_compiles_allowlisted_grouped_query_with_parameterized_values(self):
        malicious_value = "51A' OR TRUE --"
        compiled = compile_query(
            "test-project",
            "trip_observations",
            "2026-11-01",
            "2026-11-07",
            columns=["route_id"],
            aggregations=[
                {"operation": "avg", "column": "delay_seconds", "alias": "average_delay"},
                {"operation": "count", "alias": "arrival_rows"},
            ],
            filters=[{"column": "route_id", "operator": "eq", "value": malicious_value}],
            group_by=["route_id"],
            order_by=[{"column": "average_delay", "direction": "desc"}],
            limit=20,
        )

        self.assertNotIn(malicious_value, compiled.sql)
        self.assertIn("`route_id` = @filter_0", compiled.sql)
        self.assertIn("service_date BETWEEN @start_date AND @end_date", compiled.sql)
        self.assertIn("QUALIFY ROW_NUMBER()", compiled.sql)
        self.assertEqual(malicious_value, compiled.parameters[-1].value)
        self.assertEqual(["route_id", "average_delay", "arrival_rows"], compiled.result_columns)

    def test_rejects_identifier_and_alias_injection(self):
        with self.assertRaisesRegex(ValueError, "column must be one of"):
            compile_query(
                "test-project",
                "trip_observations",
                "2026-11-01",
                "2026-11-01",
                columns=["route_id FROM another_table"],
            )
        with self.assertRaisesRegex(ValueError, "aggregation aliases"):
            compile_query(
                "test-project",
                "trip_observations",
                "2026-11-01",
                "2026-11-01",
                aggregations=[
                    {"operation": "count", "alias": "n) FROM secret --"}
                ],
            )

    def test_rejects_unbounded_or_unsupported_shapes(self):
        with self.assertRaisesRegex(ValueError, "service_date filter"):
            compile_query(
                "test-project",
                "trip_probes",
                "2026-11-01",
                "2026-11-01",
                columns=["trip_id"],
                filters=[
                    {"column": "service_date", "operator": "eq", "value": "2026-11-01"}
                ],
            )
        with self.assertRaisesRegex(ValueError, "numeric column"):
            compile_query(
                "test-project",
                "trip_observations",
                "2026-11-01",
                "2026-11-01",
                aggregations=[
                    {"operation": "avg", "column": "route_id", "alias": "bad_average"}
                ],
            )

    def test_date_scope_defaults_and_caps_completed_days(self):
        resolved = resolve_query_dates(None, None, date(2026, 11, 20))

        self.assertEqual(("2026-11-13", "2026-11-19"), (resolved.start_date, resolved.end_date))
        self.assertTrue(resolved.timeframe["assumed"])
        with self.assertRaisesRegex(ValueError, "31 calendar days"):
            resolve_query_dates("2026-10-01", "2026-11-01", date(2026, 11, 20))
        with self.assertRaisesRegex(ValueError, "completed service dates"):
            resolve_query_dates("2026-11-20", "2026-11-20", date(2026, 11, 20))

    def test_schema_omits_internal_error_text_and_disallows_sql(self):
        schema = describe_schema("api_request_observations")
        columns = {item["name"] for item in schema["tables"][0]["columns"]}

        self.assertNotIn("error_message", columns)
        self.assertFalse(schema["query_contract"]["raw_sql_accepted"])

    def test_allowlisted_columns_match_terraform_schemas(self):
        schema_dir = Path(__file__).resolve().parents[3] / "infra" / "schemas"

        for table_name, table in TABLES.items():
            deployed = {
                item["name"]: item["type"]
                for item in json.loads((schema_dir / f"{table_name}.json").read_text())
            }
            self.assertEqual(
                {name: column.type for name, column in table.columns.items()},
                {name: deployed[name] for name in table.columns},
            )


class StructuredVisualizationTest(unittest.TestCase):
    def test_builds_pie_chart_from_bounded_result(self):
        charts, maps, warnings = build_artifacts(
            "api_request_observations",
            [
                {"outcome": "success", "requests": 90},
                {"outcome": "timeout", "requests": 10},
            ],
            ["outcome", "requests"],
            {
                "type": "pie",
                "title": "Request outcomes",
                "x_column": "outcome",
                "value_columns": ["requests"],
            },
        )

        self.assertEqual([], maps)
        self.assertEqual([], warnings)
        self.assertEqual("pie", charts[0]["type"])
        self.assertEqual([90, 10], charts[0]["series"][0]["values"])

    def test_builds_general_heatmap(self):
        charts, _, _ = build_artifacts(
            "trip_observations",
            [
                {"service_date": "2026-11-02", "direction_id": 0, "delay": 3.5},
                {"service_date": "2026-11-02", "direction_id": 1, "delay": 5.0},
            ],
            ["service_date", "direction_id", "delay"],
            {
                "type": "heatmap",
                "x_column": "service_date",
                "y_column": "direction_id",
                "value_columns": ["delay"],
            },
        )

        self.assertEqual("Delay", charts[0]["value_label"])
        self.assertEqual(3.5, charts[0]["min_value"])
        self.assertEqual(5, charts[0]["max_value"])

    def test_builds_bounded_geojson_point_map(self):
        _, maps, _ = build_artifacts(
            "trip_probes",
            [{"lat": 37.8, "lon": -122.2, "trip_id": "t1", "speed": 5.2}],
            ["lat", "lon", "trip_id", "speed"],
            {
                "type": "map",
                "latitude_column": "lat",
                "longitude_column": "lon",
                "label_column": "trip_id",
                "value_columns": ["speed"],
            },
        )

        self.assertEqual([-122.2, 37.8], maps[0]["geojson"]["features"][0]["geometry"]["coordinates"])
        self.assertEqual("stop", maps[0]["style"]["target"])


if __name__ == "__main__":
    unittest.main()
