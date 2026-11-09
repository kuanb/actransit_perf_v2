from __future__ import annotations

import csv
import io
import json
import os
import re
import threading
import time as monotonic_time
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from typing import Any, Literal
from zoneinfo import ZoneInfo

from google.cloud import bigquery, storage

from core import (
    METRICS,
    ROUTE_GROUPS,
    aggregate_metric,
    comparable_route_group,
    daily_metric_value,
    default_analysis_window,
    format_metric,
    number,
    resolve_daily_dates,
    route_ids_for_group,
    route_group_source,
    system_metric_row,
)


PACIFIC = ZoneInfo("America/Los_Angeles")
ROUTE_PATTERN = re.compile(r"^[A-Za-z0-9-]{1,16}$")
TIME_PATTERN = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
PROFILE_METRICS = {
    "median_delay": ("Median delay by hour", "p50", "Minutes"),
    "p95_delay": ("95th percentile delay by hour", "p95", "Minutes"),
    "observed_arrivals": ("Observed stop arrivals by hour", "n", "Observed arrivals"),
}
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
STOP_MAP_METRICS = {
    "median_delay": ("Median delay", "p50_delay_s", "minutes"),
    "p95_delay": ("95th-percentile delay", "p95_delay_s", "minutes"),
    "delay_variability": ("Delay variability", "stddev_delay_s", "minutes standard deviation"),
}
SEGMENT_MAP_METRICS = {
    "median_schedule_loss": ("Median time added versus schedule", "p50_schedule_loss_s", "minutes"),
    "p95_schedule_loss": ("95th-percentile time added versus schedule", "p95_schedule_loss_s", "minutes"),
    "median_travel_time": ("Median stop-to-stop travel time", "p50_travel_time_s", "minutes"),
    "p95_travel_time": ("95th-percentile stop-to-stop travel time", "p95_travel_time_s", "minutes"),
    "median_speed": ("Median stop-to-stop speed", "p50_speed_mps", "mph"),
}


class TransitData:
    def __init__(self) -> None:
        self.project_id = os.environ.get("PROJECT_ID", "transit-203605")
        self.bucket_name = os.environ.get("CACHE_BUCKET", f"{self.project_id}-actransit-cache")
        self.maximum_bytes_billed = int(os.environ.get("BQ_MAXIMUM_BYTES_BILLED", str(100 * 1024 * 1024)))
        self.storage = storage.Client(project=self.project_id)
        self.bucket = self.storage.bucket(self.bucket_name)
        self.bigquery = bigquery.Client(project=self.project_id, location=os.environ.get("BQ_LOCATION", "us-west1"))
        self._json_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._json_cache_lock = threading.Lock()

    def read_json(self, key: str) -> dict[str, Any]:
        now = monotonic_time.monotonic()
        with self._json_cache_lock:
            cached = self._json_cache.get(key)
            if cached and cached[0] > now:
                return cached[1]
        value = self._download_json(key)
        ttl = 30 if key.endswith("/latest.json") or key.endswith("_index.json") else 300
        with self._json_cache_lock:
            if len(self._json_cache) >= 96 and key not in self._json_cache:
                oldest = min(self._json_cache, key=lambda item: self._json_cache[item][0])
                self._json_cache.pop(oldest)
            self._json_cache[key] = (now + ttl, value)
        return value

    def _download_json(self, key: str) -> dict[str, Any]:
        return json.loads(self.bucket.blob(key).download_as_bytes())

    def published_dates(self) -> list[str]:
        return list(self.read_json("stats/_index.json").get("dates") or [])

    def daily_reports(self, dates: list[str]) -> list[dict[str, Any]]:
        return [self.read_json(f"stats/{service_date}.json") for service_date in dates]

    def compare_routes(
        self,
        route_ids: list[str],
        metric: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        route_ids = validate_route_ids(route_ids, minimum=2, maximum=5)
        route_group = comparable_route_group(route_ids)
        validate_metric(metric)
        resolved = resolve_daily_dates(self.published_dates(), start_date, end_date)
        reports = self.daily_reports(resolved.dates)
        rows: list[dict[str, Any]] = []
        series: list[dict[str, Any]] = []

        for route_id in route_ids:
            daily_routes = [find_route(report, route_id) for report in reports]
            available = [route for route in daily_routes if route]
            value, days = aggregate_metric(available, metric)
            rows.append(
                {
                    "route_id": route_id,
                    "route_group": route_group,
                    "route_group_label": ROUTE_GROUPS[route_group]["label"],
                    "value": value,
                    "formatted_value": format_metric(value, metric),
                    "days": days,
                }
            )
            series.append(
                {
                    "name": f"Route {route_id}",
                    "values": [daily_metric_value(route, metric) if route else None for route in daily_routes],
                }
            )

        warnings = list(resolved.warnings)
        if METRICS[metric]["kind"] == "daily_mean":
            warnings.append("Delay percentiles are summarized as the mean of each published daily percentile, not recomputed from raw observations.")
        answer_values = ", ".join(f"Route {row['route_id']}: {row['formatted_value']}" for row in rows)
        return tool_result(
            answer_context=(
                f"{ROUTE_GROUPS[route_group]['label']} {METRICS[metric]['label']} comparison. "
                f"{answer_values}."
            ),
            timeframe=resolved.timeframe,
            charts=[
                {
                    "type": "line",
                    "title": f"{METRICS[metric]['label']} by service date",
                    "x_label": "Service date",
                    "labels": resolved.dates,
                    "y_label": METRICS[metric]["unit"].title(),
                    "series": series,
                }
            ],
            tables=[
                {
                    "title": f"{METRICS[metric]['label']} comparison",
                    "columns": [
                        {"key": "route_id", "label": "Route"},
                        {"key": "route_group_label", "label": "Service type"},
                        {"key": "formatted_value", "label": METRICS[metric]["label"]},
                        {"key": "days", "label": "Service days"},
                    ],
                    "rows": rows,
                }
            ],
            sources=[daily_source(resolved.dates), route_group_source(route_group)],
            warnings=warnings + [ROUTE_GROUPS[route_group]["definition"]],
        )

    def get_route_performance(
        self,
        route_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        route_id = validate_route_id(route_id)
        resolved = resolve_daily_dates(self.published_dates(), start_date, end_date)
        reports = self.daily_reports(resolved.dates)
        routes = [find_route(report, route_id) for report in reports]
        available = [route for route in routes if route]
        if not available:
            raise ValueError(f"Route {route_id} has no published data in the selected period")

        metrics = [
            "on_time_service_delivered",
            "trip_delivery",
            "on_time_within_3_minutes",
            "median_delay",
            "p95_delay",
            "average_speed",
            "dropped_trips",
            "bunched_headways",
            "long_gaps",
        ]
        summary_rows = []
        for metric in metrics:
            value, days = aggregate_metric(available, metric)
            summary_rows.append(
                {
                    "metric": METRICS[metric]["label"],
                    "value": format_metric(value, metric),
                    "service_days": days,
                }
            )

        delivery_metrics = ["on_time_service_delivered", "trip_delivery"]
        charts = [
            {
                "type": "line",
                "title": f"Route {route_id} service delivery by day",
                "x_label": "Service date",
                "labels": resolved.dates,
                "y_label": "Percent",
                "series": [
                    {
                        "name": METRICS[metric]["label"],
                        "values": [daily_metric_value(route, metric) if route else None for route in routes],
                    }
                    for metric in delivery_metrics
                ],
            },
            {
                "type": "line",
                "title": f"Route {route_id} delay by day",
                "x_label": "Service date",
                "labels": resolved.dates,
                "y_label": "Minutes",
                "series": [
                    {
                        "name": METRICS[metric]["label"],
                        "values": [daily_metric_value(route, metric) if route else None for route in routes],
                    }
                    for metric in ["median_delay", "p95_delay"]
                ],
            },
        ]
        headline = "; ".join(f"{row['metric']}: {row['value']}" for row in summary_rows[:4])
        return tool_result(
            answer_context=f"Route {route_id} summary. {headline}.",
            timeframe=resolved.timeframe,
            charts=charts,
            tables=[
                {
                    "title": f"Route {route_id} performance summary",
                    "columns": [
                        {"key": "metric", "label": "Metric"},
                        {"key": "value", "label": "Value"},
                        {"key": "service_days", "label": "Service days"},
                    ],
                    "rows": summary_rows,
                }
            ],
            sources=[daily_source(resolved.dates)],
            warnings=resolved.warnings + ["Delay percentiles are summarized from published daily percentiles."],
        )

    def get_route_delay_map(
        self,
        route_id: str,
        metric: str = "median_delay",
        start_date: str | None = None,
        end_date: str | None = None,
        direction_id: int | None = None,
    ) -> dict[str, Any]:
        route_id = validate_route_id(route_id)
        validate_direction_id(direction_id)
        if metric not in STOP_MAP_METRICS:
            raise ValueError(f"metric must be one of: {', '.join(STOP_MAP_METRICS)}")
        resolved = self.resolve_map_dates(start_date, end_date)
        route = self.read_json(f"gtfs/processed/route_{route_id}.json")
        rows = self.query_stop_map_stats(
            route_id,
            resolved.dates,
            direction_id,
        )
        label, field, unit = STOP_MAP_METRICS[metric]
        grouped: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
        ranked = []
        for row in rows:
            if row.get("direction_id") is None or row.get(field) is None:
                continue
            direction = int(row["direction_id"])
            value = round(float(row[field]) / 60.0, 1)
            stop_id = str(row["stop_id"])
            stop = (route.get("stops") or {}).get(stop_id) or {}
            normalized = {
                "direction_id": direction,
                "stop_id": stop_id,
                "stop_name": stop.get("stop_name") or stop_id,
                "metric_value": value,
                "metric_display": format_map_value(value, unit),
                "observations": int(row.get("observations") or 0),
            }
            grouped[direction][stop_id] = normalized
            ranked.append(normalized)
        if not ranked:
            raise ValueError(f"Route {route_id} has no mapped stop-delay observations in the selected period")

        style = map_style(metric, [row["metric_value"] for row in ranked])
        maps = [
            build_stop_performance_map(
                route_id,
                route,
                direction,
                grouped[direction],
                label,
                style,
                100 - index,
            )
            for index, direction in enumerate(sorted(grouped))
        ]
        ranked.sort(key=lambda row: row["metric_value"], reverse=True)
        leaders = ", ".join(
            f"direction {row['direction_id']} at {row['stop_name']} ({row['metric_display']})"
            for row in ranked[:5]
        )
        return tool_result(
            answer_context=f"Route {route_id} {label.lower()} is highest at {leaders}.",
            timeframe=resolved.timeframe,
            maps=maps,
            charts=[],
            tables=[
                {
                    "title": f"Route {route_id} highest {label.lower()} stops",
                    "columns": [
                        {"key": "direction_id", "label": "Direction"},
                        {"key": "stop_name", "label": "Stop"},
                        {"key": "metric_display", "label": label},
                        {"key": "observations", "label": "Observed arrivals"},
                    ],
                    "rows": ranked[:20],
                }
            ],
            sources=[
                spatial_bigquery_source(self.project_id, resolved.dates),
                processed_gtfs_source(route_id, route),
            ],
            warnings=resolved.warnings
            + [
                "Map geometry and stop locations use the current processed GTFS feed; the performance values use the selected service dates.",
                "Each direction map includes the current scheduled patterns and stops, including stops without enough observations to calculate the selected metric.",
            ],
        )

    def get_route_segment_performance_map(
        self,
        route_id: str,
        metric: str = "median_schedule_loss",
        start_date: str | None = None,
        end_date: str | None = None,
        direction_id: int | None = None,
    ) -> dict[str, Any]:
        route_id = validate_route_id(route_id)
        validate_direction_id(direction_id)
        if metric not in SEGMENT_MAP_METRICS:
            raise ValueError(f"metric must be one of: {', '.join(SEGMENT_MAP_METRICS)}")
        resolved = self.resolve_map_dates(start_date, end_date)
        route = self.read_json(f"gtfs/processed/route_{route_id}.json")
        rows = self.query_segment_map_stats(
            route_id,
            resolved.dates,
            direction_id,
            metric,
        )
        label, field, unit = SEGMENT_MAP_METRICS[metric]
        grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
        ranked = []
        for row in rows:
            if row.get("direction_id") is None or row.get(field) is None:
                continue
            direction = int(row["direction_id"])
            raw_value = float(row[field])
            value = raw_value * 2.236936 if metric == "median_speed" else raw_value / 60.0
            from_stop_id = str(row["from_stop_id"])
            to_stop_id = str(row["to_stop_id"])
            stops = route.get("stops") or {}
            normalized = {
                "direction_id": direction,
                "from_stop_id": from_stop_id,
                "from_stop_name": (stops.get(from_stop_id) or {}).get("stop_name") or from_stop_id,
                "to_stop_id": to_stop_id,
                "to_stop_name": (stops.get(to_stop_id) or {}).get("stop_name") or to_stop_id,
                "metric_value": round(value, 1),
                "metric_display": format_map_value(value, unit),
                "observations": int(row.get("observations") or 0),
            }
            grouped[direction].append(normalized)
            ranked.append(normalized)
        if not ranked:
            raise ValueError(f"Route {route_id} has no mapped segment observations in the selected period")

        style = map_style(metric, [row["metric_value"] for row in ranked])
        maps = [
            build_segment_performance_map(
                route_id,
                route,
                direction,
                grouped[direction],
                label,
                style,
                100 - index,
            )
            for index, direction in enumerate(sorted(grouped))
        ]
        reverse = metric != "median_speed"
        ranked.sort(key=lambda row: row["metric_value"], reverse=reverse)
        leaders = ", ".join(
            f"direction {row['direction_id']} from {row['from_stop_name']} to {row['to_stop_name']} ({row['metric_display']})"
            for row in ranked[:5]
        )
        return tool_result(
            answer_context=f"Route {route_id} {label.lower()} is most notable on {leaders}.",
            timeframe=resolved.timeframe,
            maps=maps,
            charts=[],
            tables=[
                {
                    "title": f"Route {route_id} stop-to-stop {label.lower()}",
                    "columns": [
                        {"key": "direction_id", "label": "Direction"},
                        {"key": "from_stop_name", "label": "From"},
                        {"key": "to_stop_name", "label": "To"},
                        {"key": "metric_display", "label": label},
                        {"key": "observations", "label": "Observed trips"},
                    ],
                    "rows": ranked[:20],
                }
            ],
            sources=[
                spatial_bigquery_source(self.project_id, resolved.dates),
                processed_gtfs_source(route_id, route),
            ],
            warnings=resolved.warnings
            + [
                "Sections run between consecutive scheduled stops. These observations cannot isolate delay at a particular turn or intersection inside a section.",
                "Longer sections naturally have longer absolute travel times; time added versus schedule or median speed is better for comparing differently sized sections.",
                "Map geometry and stop locations use the current processed GTFS feed; the performance values use the selected service dates.",
            ],
        )

    def resolve_map_dates(
        self,
        start_date: str | None,
        end_date: str | None,
    ):
        resolved = resolve_daily_dates(self.published_dates(), start_date, end_date)
        start = date.fromisoformat(str(resolved.timeframe["start_date"]))
        end = date.fromisoformat(str(resolved.timeframe["end_date"]))
        if (end - start).days > 6:
            raise ValueError("Route maps are limited to seven calendar days")
        return resolved

    def query_stop_map_stats(
        self,
        route_id: str,
        service_dates: list[str],
        direction_id: int | None,
    ) -> list[dict[str, Any]]:
        sql = f"""
            WITH obs AS (
              SELECT
                service_date,
                trip_id,
                stop_sequence,
                ingested_at,
                direction_id,
                stop_id,
                delay_seconds,
                is_stale
              FROM `{self.project_id}.actransit.trip_observations`
              WHERE service_date IN UNNEST(@service_dates)
                AND route_id = @route_id
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY service_date, trip_id, stop_sequence
                ORDER BY IF(delay_seconds IS NULL, 1, 0), ingested_at DESC
              ) = 1
            )
            SELECT
              direction_id,
              stop_id,
              COUNT(*) AS observations,
              APPROX_QUANTILES(delay_seconds, 100)[OFFSET(50)] AS p50_delay_s,
              APPROX_QUANTILES(delay_seconds, 100)[OFFSET(95)] AS p95_delay_s,
              STDDEV(CAST(delay_seconds AS FLOAT64)) AS stddev_delay_s
            FROM obs
            WHERE delay_seconds IS NOT NULL
              AND is_stale = FALSE
              AND (@direction_id IS NULL OR direction_id = @direction_id)
            GROUP BY direction_id, stop_id
            ORDER BY direction_id, stop_id
        """
        return self.run_spatial_query(sql, route_id, service_dates, direction_id)

    def query_segment_map_stats(
        self,
        route_id: str,
        service_dates: list[str],
        direction_id: int | None,
        metric: str,
    ) -> list[dict[str, Any]]:
        source_field = {
            "median_schedule_loss": "delay_seconds",
            "p95_schedule_loss": "delay_seconds",
            "median_travel_time": "leg_duration_s",
            "p95_travel_time": "leg_duration_s",
            "median_speed": "leg_avg_speed_mps",
        }[metric]
        result_field = SEGMENT_MAP_METRICS[metric][1]
        percentile = 95 if metric.startswith("p95_") else 50
        if metric.endswith("schedule_loss"):
            previous_metric = ", LAG(metric_source) OVER trip_stops AS previous_metric_source"
            metric_expression = "metric_source - previous_metric_source"
            metric_filters = "AND previous_metric_source IS NOT NULL AND ABS(metric_source - previous_metric_source) <= 7200"
        elif metric.endswith("travel_time"):
            previous_metric = ""
            metric_expression = "metric_source"
            metric_filters = "AND metric_source BETWEEN 1 AND 7200"
        else:
            previous_metric = ""
            metric_expression = "metric_source"
            metric_filters = "AND metric_source BETWEEN 0 AND 35"
        sql = f"""
            WITH obs AS (
              SELECT
                service_date,
                trip_id,
                stop_sequence,
                ingested_at,
                direction_id,
                stop_id,
                {source_field} AS metric_source,
                is_stale
              FROM `{self.project_id}.actransit.trip_observations`
              WHERE service_date IN UNNEST(@service_dates)
                AND route_id = @route_id
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY service_date, trip_id, stop_sequence
                ORDER BY IF({source_field} IS NULL, 1, 0), ingested_at DESC
              ) = 1
            ), sequenced AS (
              SELECT
                direction_id,
                stop_id AS to_stop_id,
                LAG(stop_id) OVER trip_stops AS from_stop_id,
                metric_source,
                is_stale
                {previous_metric}
              FROM obs
              WINDOW trip_stops AS (
                PARTITION BY service_date, trip_id
                ORDER BY stop_sequence
              )
            )
            SELECT
              direction_id,
              from_stop_id,
              to_stop_id,
              COUNT(*) AS observations,
              APPROX_QUANTILES({metric_expression}, 100)[OFFSET({percentile})] AS {result_field}
            FROM sequenced
            WHERE from_stop_id IS NOT NULL
              AND metric_source IS NOT NULL
              AND is_stale = FALSE
              {metric_filters}
              AND (@direction_id IS NULL OR direction_id = @direction_id)
            GROUP BY direction_id, from_stop_id, to_stop_id
            ORDER BY direction_id, from_stop_id, to_stop_id
        """
        return self.run_spatial_query(sql, route_id, service_dates, direction_id)

    def run_spatial_query(
        self,
        sql: str,
        route_id: str,
        service_dates: list[str],
        direction_id: int | None,
    ) -> list[dict[str, Any]]:
        config = bigquery.QueryJobConfig(
            maximum_bytes_billed=self.maximum_bytes_billed,
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "service_dates",
                    "DATE",
                    [date.fromisoformat(value) for value in service_dates],
                ),
                bigquery.ScalarQueryParameter("route_id", "STRING", route_id),
                bigquery.ScalarQueryParameter("direction_id", "INT64", direction_id),
            ],
        )
        return [dict(row.items()) for row in self.bigquery.query(sql, job_config=config).result(timeout=20)]

    def get_route_service_profile(
        self,
        route_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        route_id = validate_route_id(route_id)
        resolved = resolve_daily_dates(self.published_dates(), start_date, end_date)
        reports = self.daily_reports(resolved.dates)
        routes = [find_route(report, route_id) for report in reports]
        if not any(routes):
            raise ValueError(f"Route {route_id} has no published data in the selected period")

        daily_rows = []
        for service_date, route in zip(resolved.dates, routes):
            scheduled = int(number((route or {}).get("scheduled_trips")))
            ran = int(number((route or {}).get("ran_trips")))
            dropped = max(0, scheduled - ran)
            daily_rows.append(
                {
                    "service_date": service_date,
                    "scheduled_trips": scheduled,
                    "dropped_trips": dropped,
                    "trip_delivery": round(ran / scheduled * 100, 1) if scheduled else None,
                }
            )

        observed = self.query_observed_trips(route_id, resolved.dates[0], resolved.dates[-1])
        observed_by_date: dict[str, set[str]] = defaultdict(set)
        for row in observed:
            service_date = row["service_date"]
            date_key = service_date.isoformat() if hasattr(service_date, "isoformat") else str(service_date)
            observed_by_date[date_key].add(str(row["trip_id"]))

        bodies = self.gtfs_for_dates(resolved.dates)
        templates_by_body: dict[int, dict[str, list[dict[str, Any]]]] = {}
        hour_cells: dict[tuple[str, int], dict[str, int]] = defaultdict(
            lambda: {"scheduled": 0, "dropped": 0}
        )
        for service_date in resolved.dates:
            body = bodies[service_date]
            body_key = id(body)
            if body_key not in templates_by_body:
                templates_by_body[body_key] = route_trip_templates(body, route_id)
            templates = templates_by_body[body_key]
            active = active_services(body, service_date)
            base_date = date.fromisoformat(service_date)
            for service_id in active:
                for trip in templates.get(service_id, []):
                    start_seconds = int(trip["start_seconds"])
                    start_day = base_date + timedelta(days=start_seconds // 86400)
                    weekday = WEEKDAYS[start_day.weekday()]
                    hour = (start_seconds // 3600) % 24
                    cell = hour_cells[(weekday, hour)]
                    cell["scheduled"] += 1
                    if trip["trip_id"] not in observed_by_date[service_date]:
                        cell["dropped"] += 1

        heatmap_cells = []
        for weekday in WEEKDAYS:
            for hour in range(24):
                counts = hour_cells[(weekday, hour)]
                scheduled = counts["scheduled"]
                dropped = counts["dropped"]
                heatmap_cells.append(
                    {
                        "x": f"{hour:02d}:00",
                        "y": weekday,
                        "value": round(dropped / scheduled * 100, 1) if scheduled else None,
                        "scheduled": scheduled,
                        "dropped": dropped,
                    }
                )

        worst_cells = sorted(
            (cell for cell in heatmap_cells if cell["dropped"] > 0),
            key=lambda cell: (cell["dropped"], cell["value"] or 0, cell["scheduled"]),
            reverse=True,
        )
        worst_text = ", ".join(
            f"{cell['y']} {cell['x']} ({cell['dropped']} of {cell['scheduled']}, {cell['value']:.1f}%)"
            for cell in worst_cells[:5]
        )
        total_scheduled = sum(row["scheduled_trips"] for row in daily_rows)
        total_dropped = sum(row["dropped_trips"] for row in daily_rows)
        daily_extremes = sorted(daily_rows, key=lambda row: row["dropped_trips"], reverse=True)
        answer_context = (
            f"Route {route_id} scheduled and dropped-trip profile. "
            f"Across {len(daily_rows)} published service days, {total_dropped} of {total_scheduled} scheduled trips "
            "were Dropped / not observed. "
        )
        if daily_extremes:
            answer_context += (
                f"The highest daily count was {daily_extremes[0]['dropped_trips']} on "
                f"{daily_extremes[0]['service_date']}. "
            )
        if worst_text:
            answer_context += f"Worst scheduled start periods by absolute dropped trips: {worst_text}."

        return tool_result(
            answer_context=answer_context,
            timeframe=resolved.timeframe,
            charts=[
                {
                    "type": "bar",
                    "priority": 100,
                    "title": f"Route {route_id} scheduled and dropped trips by day",
                    "x_label": "Service date",
                    "labels": resolved.dates,
                    "y_label": "Trips",
                    "series": [
                        {
                            "name": "Scheduled trips",
                            "values": [row["scheduled_trips"] for row in daily_rows],
                        },
                        {
                            "name": "Dropped / not observed trips",
                            "values": [row["dropped_trips"] for row in daily_rows],
                        },
                    ],
                },
                {
                    "type": "heatmap",
                    "priority": 95,
                    "title": f"Route {route_id} dropped trips by scheduled start hour",
                    "x_label": "Scheduled start hour in Pacific time",
                    "y_label": "Scheduled start weekday",
                    "x_labels": [f"{hour:02d}:00" for hour in range(24)],
                    "y_labels": WEEKDAYS,
                    "value_label": "Dropped share",
                    "cells": heatmap_cells,
                    "note": "Cell labels show the number of dropped trips; color represents the dropped share of scheduled trips.",
                },
            ],
            tables=[
                {
                    "title": f"Route {route_id} scheduled and dropped trips by day",
                    "columns": [
                        {"key": "service_date", "label": "Service date"},
                        {"key": "scheduled_trips", "label": "Scheduled trips"},
                        {"key": "dropped_trips", "label": "Dropped / not observed"},
                        {"key": "trip_delivery", "label": "Trip delivery (%)"},
                    ],
                    "rows": daily_rows,
                },
                {
                    "title": f"Route {route_id} worst scheduled start periods",
                    "columns": [
                        {"key": "y", "label": "Weekday"},
                        {"key": "x", "label": "Hour (PT)"},
                        {"key": "scheduled", "label": "Scheduled trips"},
                        {"key": "dropped", "label": "Dropped / not observed"},
                        {"key": "value", "label": "Dropped share (%)"},
                    ],
                    "rows": worst_cells[:24],
                },
            ],
            sources=[
                daily_source(resolved.dates),
                {
                    "kind": "gtfs_schedule",
                    "start_date": resolved.dates[0],
                    "end_date": resolved.dates[-1],
                },
                {
                    "kind": "bigquery",
                    "table": f"{self.project_id}.actransit.trip_observations",
                    "start_date": resolved.dates[0],
                    "end_date": resolved.dates[-1],
                    "deduplicated": True,
                },
            ],
            warnings=resolved.warnings
            + [
                "Hourly cells assign each trip to its scheduled start weekday and hour in Pacific time; after-midnight GTFS times appear on the following calendar day."
            ],
        )

    def get_system_performance(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        resolved = resolve_daily_dates(self.published_dates(), start_date, end_date)
        reports = self.daily_reports(resolved.dates)
        systems = [system_metric_row(report) for report in reports]
        metrics = [
            "on_time_service_delivered",
            "trip_delivery",
            "on_time_within_3_minutes",
            "median_delay",
            "p95_delay",
            "average_speed",
            "dropped_trips",
            "bunched_headways",
            "long_gaps",
        ]
        summary_rows = []
        for metric in metrics:
            value, days = aggregate_metric(systems, metric)
            summary_rows.append(
                {
                    "metric": METRICS[metric]["label"],
                    "value": format_metric(value, metric),
                    "service_days": days,
                }
            )

        charts = [
            {
                "type": "line",
                "title": "Systemwide service delivery by day",
                "x_label": "Service date",
                "labels": resolved.dates,
                "y_label": "Percent",
                "series": [
                    {
                        "name": METRICS[metric]["label"],
                        "values": [daily_metric_value(system, metric) for system in systems],
                    }
                    for metric in ["on_time_service_delivered", "trip_delivery"]
                ],
            },
            {
                "type": "line",
                "title": "Systemwide delay by day",
                "x_label": "Service date",
                "labels": resolved.dates,
                "y_label": "Minutes",
                "series": [
                    {
                        "name": METRICS[metric]["label"],
                        "values": [daily_metric_value(system, metric) for system in systems],
                    }
                    for metric in ["median_delay", "p95_delay"]
                ],
            },
        ]
        headline = "; ".join(f"{row['metric']}: {row['value']}" for row in summary_rows[:4])
        return tool_result(
            answer_context=f"AC Transit systemwide summary. {headline}.",
            timeframe=resolved.timeframe,
            charts=charts,
            tables=[
                {
                    "title": "Systemwide performance summary",
                    "columns": [
                        {"key": "metric", "label": "Metric"},
                        {"key": "value", "label": "Value"},
                        {"key": "service_days", "label": "Service days"},
                    ],
                    "rows": summary_rows,
                }
            ],
            sources=[daily_source(resolved.dates)],
            warnings=resolved.warnings + ["Delay percentiles are summarized from published daily percentiles."],
        )

    def rank_routes(
        self,
        metric: str,
        start_date: str | None = None,
        end_date: str | None = None,
        order: Literal["worst", "best"] = "worst",
        limit: int = 10,
        route_group: Literal["regular", "school", "all_nighter", "transbay"] = "regular",
    ) -> dict[str, Any]:
        validate_metric(metric)
        if route_group not in ROUTE_GROUPS:
            raise ValueError(f"route_group must be one of: {', '.join(ROUTE_GROUPS)}")
        if limit < 1 or limit > 10:
            raise ValueError("limit must be between 1 and 10")
        if order not in {"worst", "best"}:
            raise ValueError("order must be worst or best")
        resolved = resolve_daily_dates(self.published_dates(), start_date, end_date)
        reports = self.daily_reports(resolved.dates)
        route_ids = route_ids_for_group(
            [
                str(route["route_id"])
                for report in reports
                for route in report.get("routes") or []
            ],
            route_group,
        )
        ranked: list[dict[str, Any]] = []
        for route_id in route_ids:
            routes = [find_route(report, route_id) for report in reports]
            value, days = aggregate_metric([route for route in routes if route], metric)
            if value is not None:
                ranked.append(
                    {
                        "route_id": route_id,
                        "route_group": route_group,
                        "route_group_label": ROUTE_GROUPS[route_group]["label"],
                        "value": value,
                        "days": days,
                    }
                )

        descending_for_worst = bool(METRICS[metric]["higher_is_worse"])
        reverse = descending_for_worst if order == "worst" else not descending_for_worst
        ranked.sort(key=lambda row: (row["value"], row["route_id"]), reverse=reverse)
        ranked = ranked[:limit]
        for position, row in enumerate(ranked, start=1):
            row["rank"] = position
            row["formatted_value"] = format_metric(row["value"], metric)

        return tool_result(
            answer_context=(
                f"{order.title()} {ROUTE_GROUPS[route_group]['label']} routes for {METRICS[metric]['label']}: "
                + ", ".join(f"{row['route_id']} ({row['formatted_value']})" for row in ranked)
                + "."
            ),
            timeframe=resolved.timeframe,
            charts=[
                {
                    "type": "bar",
                    "title": (
                        f"{order.title()} {ROUTE_GROUPS[route_group]['label']} routes: "
                        f"{METRICS[metric]['label']}"
                    ),
                    "x_label": "Route",
                    "labels": [row["route_id"] for row in ranked],
                    "y_label": METRICS[metric]["unit"].title(),
                    "series": [{"name": METRICS[metric]["label"], "values": [row["value"] for row in ranked]}],
                }
            ],
            tables=[
                {
                    "title": f"{order.title()} {ROUTE_GROUPS[route_group]['label']} routes",
                    "columns": [
                        {"key": "rank", "label": "Rank"},
                        {"key": "route_id", "label": "Route"},
                        {"key": "route_group_label", "label": "Service type"},
                        {"key": "formatted_value", "label": METRICS[metric]["label"]},
                    ],
                    "rows": ranked,
                }
            ],
            sources=[daily_source(resolved.dates), route_group_source(route_group)],
            warnings=resolved.warnings + [ROUTE_GROUPS[route_group]["definition"]],
        )

    def get_route_time_profile(
        self,
        route_id: str,
        metric: str = "median_delay",
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        route_id = validate_route_id(route_id)
        if metric not in PROFILE_METRICS:
            raise ValueError(f"metric must be one of: {', '.join(PROFILE_METRICS)}")

        if start_date is None and end_date is None:
            start_date, end_date = default_analysis_window()
            assumed = True
            disclosure = (
                "No timeframe was provided, so this hourly profile uses the seven-day period ending yesterday: "
                f"{start_date} through {end_date}."
            )
            rows = self.query_hourly_profile(route_id, start_date, end_date)
            source = {
                "kind": "bigquery",
                "table": f"{self.project_id}.actransit.trip_observations",
                "start_date": start_date,
                "end_date": end_date,
                "deduplicated": True,
            }
        else:
            if start_date is None:
                start_date = end_date
            if end_date is None:
                end_date = start_date
            validate_date_range(start_date, end_date)
            assumed = False
            disclosure = f"Analyzed observed arrivals from {start_date} through {end_date}."
            rows = self.query_hourly_profile(route_id, start_date, end_date)
            source = {
                "kind": "bigquery",
                "table": f"{self.project_id}.actransit.trip_observations",
                "start_date": start_date,
                "end_date": end_date,
                "deduplicated": True,
            }

        if not rows:
            raise ValueError(f"Route {route_id} has no hourly observations in the selected period")
        title, field, y_label = PROFILE_METRICS[metric]
        normalized = [
            {"hour": int(row["hour"]), "p50": row.get("p50"), "p95": row.get("p95"), "n": int(row.get("n") or 0)}
            for row in rows
        ]
        return tool_result(
            answer_context=(
                f"Route {route_id} {title.lower()}. "
                + ", ".join(f"{row['hour']:02d}:00={row[field]}" for row in normalized if row.get(field) is not None)
                + "."
            ),
            timeframe={
                "start_date": start_date,
                "end_date": end_date,
                "assumed": assumed,
                "basis": "seven_day_period_ending_yesterday" if assumed else "user_supplied_or_interpreted_dates",
                "disclosure": disclosure,
            },
            charts=[
                {
                    "type": "line" if field != "n" else "bar",
                    "title": f"Route {route_id}: {title.lower()}",
                    "x_label": "Hour in Pacific time",
                    "labels": [f"{row['hour']:02d}:00" for row in normalized],
                    "y_label": y_label,
                    "series": [{"name": title, "values": [row.get(field) for row in normalized]}],
                }
            ],
            tables=[
                {
                    "title": f"Route {route_id} hourly delay profile",
                    "columns": [
                        {"key": "hour_label", "label": "Hour (PT)"},
                        {"key": "p50", "label": "Median delay (min)"},
                        {"key": "p95", "label": "P95 delay (min)"},
                        {"key": "n", "label": "Observed arrivals"},
                    ],
                    "rows": [{**row, "hour_label": f"{row['hour']:02d}:00"} for row in normalized],
                }
            ],
            sources=[source],
            warnings=[],
        )

    def query_hourly_profile(self, route_id: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        sql = f"""
            WITH obs AS (
              SELECT *
              FROM `{self.project_id}.actransit.trip_observations`
              WHERE service_date BETWEEN @start_date AND @end_date
                AND route_id = @route_id
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY service_date, trip_id, stop_sequence
                ORDER BY IF(actual_arrival IS NULL, 1, 0), ingested_at DESC
              ) = 1
            )
            SELECT
              EXTRACT(HOUR FROM DATETIME(actual_arrival, "America/Los_Angeles")) AS hour,
              ROUND(APPROX_QUANTILES(delay_seconds, 100)[OFFSET(50)] / 60.0, 1) AS p50,
              ROUND(APPROX_QUANTILES(delay_seconds, 100)[OFFSET(95)] / 60.0, 1) AS p95,
              COUNT(*) AS n
            FROM obs
            WHERE actual_arrival IS NOT NULL AND is_stale = FALSE
            GROUP BY hour
            ORDER BY hour
        """
        config = bigquery.QueryJobConfig(
            maximum_bytes_billed=self.maximum_bytes_billed,
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
                bigquery.ScalarQueryParameter("route_id", "STRING", route_id),
            ],
        )
        return [dict(row.items()) for row in self.bigquery.query(sql, job_config=config).result(timeout=20)]

    def query_observed_trips(self, route_id: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        validate_date_range(start_date, end_date)
        sql = f"""
            WITH obs AS (
              SELECT service_date, trip_id, stop_sequence, actual_arrival, ingested_at
              FROM `{self.project_id}.actransit.trip_observations`
              WHERE service_date BETWEEN @start_date AND @end_date
                AND route_id = @route_id
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY service_date, trip_id, stop_sequence
                ORDER BY IF(actual_arrival IS NULL, 1, 0), ingested_at DESC
              ) = 1
            )
            SELECT DISTINCT service_date, trip_id
            FROM obs
            WHERE actual_arrival IS NOT NULL
        """
        ranges = []
        cursor = date.fromisoformat(start_date)
        final = date.fromisoformat(end_date)
        while cursor <= final:
            chunk_end = min(cursor + timedelta(days=6), final)
            ranges.append((cursor.isoformat(), chunk_end.isoformat()))
            cursor = chunk_end + timedelta(days=1)

        def run_chunk(chunk: tuple[str, str]) -> list[dict[str, Any]]:
            config = bigquery.QueryJobConfig(
                maximum_bytes_billed=self.maximum_bytes_billed,
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_date", "DATE", chunk[0]),
                    bigquery.ScalarQueryParameter("end_date", "DATE", chunk[1]),
                    bigquery.ScalarQueryParameter("route_id", "STRING", route_id),
                ],
            )
            result = self.bigquery.query(sql, job_config=config).result(timeout=20)
            return [dict(row.items()) for row in result]

        with ThreadPoolExecutor(max_workers=len(ranges)) as executor:
            return [row for rows in executor.map(run_chunk, ranges) for row in rows]

    def list_scheduled_arrivals(
        self,
        route_id: str,
        service_date: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        direction_id: int | None = None,
        stop_id: str | None = None,
        trip_id: str | None = None,
        vehicle_id: str | None = None,
    ) -> dict[str, Any]:
        route_id = validate_route_id(route_id)
        if direction_id not in {None, 0, 1}:
            raise ValueError("direction_id must be 0 or 1")
        published_dates = sorted(self.published_dates())
        _, yesterday = default_analysis_window()
        assumed_date = service_date is None
        service_date = service_date or yesterday
        date.fromisoformat(service_date)
        if service_date not in published_dates:
            raise ValueError("Scheduled-arrival analysis is available only for complete published service dates")
        assumed_time = start_time is None and end_time is None
        start_time = start_time or "06:00"
        end_time = end_time or "10:00"
        if not TIME_PATTERN.fullmatch(start_time) or not TIME_PATTERN.fullmatch(end_time):
            raise ValueError("start_time and end_time must use HH:MM")
        start_local = datetime.combine(date.fromisoformat(service_date), time.fromisoformat(start_time), PACIFIC)
        end_local = datetime.combine(date.fromisoformat(service_date), time.fromisoformat(end_time), PACIFIC)
        if end_local <= start_local:
            raise ValueError("end_time must be later than start_time on the same service date")
        if end_local - start_local > timedelta(hours=4):
            raise ValueError("The maximum scheduled-arrival window is four hours")

        gtfs = self.gtfs_for_date(service_date)
        scheduled = scheduled_rows(gtfs, service_date, route_id, start_local, end_local, direction_id, stop_id, trip_id)
        observed = self.query_arrivals(route_id, service_date, start_local, end_local, trip_id)
        observed_by_key = {(row["trip_id"], int(row["stop_sequence"])): row for row in observed}
        observed_trips = {row["trip_id"] for row in observed if row.get("trip_observed")}
        warnings: list[str] = []
        if vehicle_id:
            warnings.append(
                "Vehicle IDs exist only for observed service. A wholly unobserved scheduled trip cannot be attributed to a vehicle."
            )

        result_rows = []
        for row in scheduled:
            observation = observed_by_key.get((row["trip_id"], row["stop_sequence"]))
            if vehicle_id and (not observation or observation.get("vehicle_id") != vehicle_id):
                continue
            actual = observation.get("actual_arrival") if observation else None
            delay = observation.get("delay_seconds") if observation else None
            rider_window = "unknown"
            if actual is not None and delay is not None:
                rider_window = "inside rider window" if -60 <= delay <= 420 else "outside rider window"
            if actual is not None:
                operation = "Observed running"
            elif row["trip_id"] in observed_trips:
                operation = "Partial or stop not observed"
            else:
                operation = "Dropped / not observed"
            result_rows.append(
                {
                    **row,
                    "scheduled_arrival": row["scheduled_arrival"].isoformat(),
                    "actual_arrival": actual.isoformat() if actual else None,
                    "vehicle_id": observation.get("vehicle_id") if observation else None,
                    "delay_minutes": round(delay / 60, 1) if delay is not None else None,
                    "rider_window_status": rider_window,
                    "trip_operation_status": operation,
                }
            )

        total_rows = len(result_rows)
        if total_rows > 250:
            result_rows = result_rows[:250]
            warnings.append(f"The result contained {total_rows} rows; only the first 250 are returned.")
        observed_count = sum(row["actual_arrival"] is not None for row in result_rows)
        missing_count = len(result_rows) - observed_count
        partial_count = sum(
            row["trip_operation_status"] == "Partial or stop not observed" for row in result_rows
        )
        dropped_count = sum(
            row["trip_operation_status"] == "Dropped / not observed" for row in result_rows
        )
        if missing_count:
            warnings.append(
                "A missing stop arrival is marked Dropped / not observed only when the trip has no observed running stops; otherwise it is marked partial or stop not observed."
            )
        buckets: dict[str, dict[str, int]] = defaultdict(lambda: {"scheduled": 0, "observed": 0, "missing": 0})
        for row in result_rows:
            ts = datetime.fromisoformat(row["scheduled_arrival"])
            minute = 0 if ts.minute < 30 else 30
            label = f"{ts.hour:02d}:{minute:02d}"
            buckets[label]["scheduled"] += 1
            buckets[label]["observed" if row["actual_arrival"] else "missing"] += 1

        disclosure_parts = []
        if assumed_date:
            disclosure_parts.append(f"yesterday's complete published service day ({service_date})")
        if assumed_time:
            disclosure_parts.append("the default 06:00–10:00 Pacific window")
        assumed = assumed_date or assumed_time
        disclosure = (
            "No complete timeframe was provided, so this analysis uses " + " and ".join(disclosure_parts) + "."
            if assumed
            else f"Analyzed scheduled arrivals on {service_date} from {start_time} to {end_time} Pacific time."
        )
        labels = sorted(buckets)
        return tool_result(
            answer_context=(
                f"Route {route_id} had {len(result_rows)} scheduled stop arrivals in the returned window; "
                f"{observed_count} have observed arrivals and {missing_count} do not. "
                f"Of the missing stop arrivals, {partial_count} belong to observed trips and {dropped_count} belong to trips with no observed running."
            ),
            timeframe={
                "start_date": service_date,
                "end_date": service_date,
                "start_time": start_time,
                "end_time": end_time,
                "timezone": "America/Los_Angeles",
                "assumed": assumed,
                "basis": "yesterday_and_morning_window" if assumed else "user_supplied_or_interpreted_timeframe",
                "disclosure": disclosure,
            },
            charts=[
                {
                    "type": "bar",
                    "title": f"Route {route_id} scheduled stop arrivals",
                    "x_label": "Scheduled time (PT)",
                    "labels": labels,
                    "y_label": "Stop arrivals",
                    "series": [
                        {"name": "Observed", "values": [buckets[label]["observed"] for label in labels]},
                        {"name": "No observed arrival", "values": [buckets[label]["missing"] for label in labels]},
                    ],
                }
            ],
            tables=[
                {
                    "title": f"Route {route_id} scheduled arrivals",
                    "columns": [
                        {"key": "scheduled_arrival", "label": "Scheduled"},
                        {"key": "actual_arrival", "label": "Observed"},
                        {"key": "stop_name", "label": "Stop"},
                        {"key": "trip_id", "label": "Trip"},
                        {"key": "vehicle_id", "label": "Vehicle"},
                        {"key": "delay_minutes", "label": "Delay (min)"},
                        {"key": "rider_window_status", "label": "Rider window"},
                        {"key": "trip_operation_status", "label": "Trip status"},
                    ],
                    "rows": result_rows,
                }
            ],
            sources=[
                {"kind": "gtfs", "service_date": service_date, "archived": True},
                {
                    "kind": "bigquery",
                    "table": f"{self.project_id}.actransit.trip_observations",
                    "service_date": service_date,
                    "deduplicated": True,
                },
            ],
            warnings=warnings,
        )

    def query_arrivals(
        self,
        route_id: str,
        service_date: str,
        start_local: datetime,
        end_local: datetime,
        trip_id: str | None,
    ) -> list[dict[str, Any]]:
        sql = f"""
            WITH obs AS (
              SELECT *
              FROM `{self.project_id}.actransit.trip_observations`
              WHERE service_date = @service_date AND route_id = @route_id
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY trip_id, stop_sequence
                ORDER BY IF(actual_arrival IS NULL, 1, 0), ingested_at DESC
              ) = 1
            ), trip_status AS (
              SELECT trip_id, LOGICAL_OR(actual_arrival IS NOT NULL) AS trip_observed
              FROM obs
              GROUP BY trip_id
            )
            SELECT obs.trip_id, stop_sequence, vehicle_id, actual_arrival, delay_seconds, trip_observed
            FROM obs
            JOIN trip_status USING (trip_id)
            WHERE scheduled_arrival >= @start_time AND scheduled_arrival < @end_time
              AND (@trip_id IS NULL OR trip_id = @trip_id)
        """
        config = bigquery.QueryJobConfig(
            maximum_bytes_billed=self.maximum_bytes_billed,
            query_parameters=[
                bigquery.ScalarQueryParameter("service_date", "DATE", service_date),
                bigquery.ScalarQueryParameter("route_id", "STRING", route_id),
                bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_local.astimezone(timezone.utc)),
                bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_local.astimezone(timezone.utc)),
                bigquery.ScalarQueryParameter("trip_id", "STRING", trip_id),
            ],
        )
        return [dict(row.items()) for row in self.bigquery.query(sql, job_config=config).result(timeout=20)]

    @lru_cache(maxsize=2)
    def gtfs_for_date(self, service_date: str) -> bytes:
        current = self.bucket.blob("gtfs/current.zip").download_as_bytes()
        if active_services(current, service_date):
            return current
        keys = sorted(
            (
                blob.name
                for blob in self.storage.list_blobs(self.bucket_name, prefix="gtfs/")
                if blob.name != "gtfs/current.zip" and blob.name.endswith(".zip")
            ),
            reverse=True,
        )
        for key in keys:
            body = self.bucket.blob(key).download_as_bytes()
            if active_services(body, service_date):
                return body
        raise ValueError(f"No current or archived GTFS feed covers {service_date}")

    def gtfs_for_dates(self, service_dates: list[str]) -> dict[str, bytes]:
        unresolved = set(service_dates)
        bodies: dict[str, bytes] = {}
        current = self.bucket.blob("gtfs/current.zip").download_as_bytes()
        for service_date in list(unresolved):
            if active_services(current, service_date):
                bodies[service_date] = current
                unresolved.remove(service_date)
        if not unresolved:
            return bodies

        keys = sorted(
            (
                blob.name
                for blob in self.storage.list_blobs(self.bucket_name, prefix="gtfs/")
                if blob.name != "gtfs/current.zip" and blob.name.endswith(".zip")
            ),
            reverse=True,
        )
        for key in keys:
            body = self.bucket.blob(key).download_as_bytes()
            for service_date in list(unresolved):
                if active_services(body, service_date):
                    bodies[service_date] = body
                    unresolved.remove(service_date)
            if not unresolved:
                return bodies
        raise ValueError(f"No current or archived GTFS feed covers {min(unresolved)}")


def validate_route_id(route_id: str) -> str:
    route_id = route_id.strip().upper()
    if not ROUTE_PATTERN.fullmatch(route_id):
        raise ValueError("route_id must contain only letters, numbers, and hyphens")
    return route_id


def validate_direction_id(direction_id: int | None) -> None:
    if direction_id not in {None, 0, 1}:
        raise ValueError("direction_id must be 0 or 1")


def route_direction_features(
    route_id: str,
    route: dict[str, Any],
    direction_id: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    trips = [
        trip
        for trip in (route.get("trips") or {}).values()
        if int(trip.get("direction_id") or 0) == direction_id
    ]
    if not trips:
        return [], []
    shape_counts = Counter(
        str(trip.get("shape_id"))
        for trip in trips
        if trip.get("shape_id") in (route.get("shapes") or {})
    )
    selected_shapes = [shape_id for shape_id, _ in shape_counts.most_common()]
    points_per_shape = max(2, 500 // max(1, len(selected_shapes)))
    route_features = []
    for shape_id in selected_shapes:
        coordinates = sample_shape_coordinates(
            (route.get("shapes") or {}).get(shape_id) or [],
            points_per_shape,
        )
        if len(coordinates) < 2:
            continue
        route_features.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coordinates},
                "properties": {
                    "feature_kind": "route",
                    "name": f"Route {route_id}",
                    "direction_id": direction_id,
                    "shape_id": shape_id,
                },
            }
        )
    stop_ids = sorted(
        {
            str(stop_time.get("stop_id"))
            for trip in trips
            for stop_time in (trip.get("stop_times") or [])
            if stop_time.get("stop_id")
        }
    )
    return route_features, stop_ids


def build_stop_performance_map(
    route_id: str,
    route: dict[str, Any],
    direction_id: int,
    stats: dict[str, dict[str, Any]],
    label: str,
    style: dict[str, Any],
    priority: int,
) -> dict[str, Any]:
    features, stop_ids = route_direction_features(route_id, route, direction_id)
    stops = route.get("stops") or {}
    for stop_id in stop_ids:
        stop = stops.get(stop_id)
        if not stop:
            continue
        values = stats.get(stop_id) or {}
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(stop["lon"]), float(stop["lat"])],
                },
                "properties": {
                    "feature_kind": "stop",
                    "name": stop.get("stop_name") or stop_id,
                    "stop_id": stop_id,
                    "direction_id": direction_id,
                    "metric_value": values.get("metric_value"),
                    "has_metric": values.get("metric_value") is not None,
                    "metric_display": values.get("metric_display") or "No observations",
                    "observations": values.get("observations") or 0,
                },
            }
        )
    return {
        "priority": priority,
        "title": f"Route {route_id} direction {direction_id}: {label.lower()} by stop",
        "description": "The line shows current scheduled route patterns; colored stops show the selected performance measure.",
        "geojson": {"type": "FeatureCollection", "features": features},
        "style": {**style, "target": "stop", "value_label": label},
        "popup_fields": [
            {"property": "name", "label": "Stop"},
            {"property": "metric_display", "label": label},
            {"property": "observations", "label": "Observed arrivals"},
        ],
    }


def build_segment_performance_map(
    route_id: str,
    route: dict[str, Any],
    direction_id: int,
    stats: list[dict[str, Any]],
    label: str,
    style: dict[str, Any],
    priority: int,
) -> dict[str, Any]:
    features, stop_ids = route_direction_features(route_id, route, direction_id)
    pair_geometries = route_pair_geometries(route, direction_id)
    for row in stats:
        coordinates = pair_geometries.get((row["from_stop_id"], row["to_stop_id"]))
        if not coordinates:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coordinates},
                "properties": {
                    "feature_kind": "segment",
                    "has_metric": True,
                    "name": f"{row['from_stop_name']} to {row['to_stop_name']}",
                    **row,
                },
            }
        )
    stops = route.get("stops") or {}
    for stop_id in stop_ids:
        stop = stops.get(stop_id)
        if not stop:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(stop["lon"]), float(stop["lat"])],
                },
                "properties": {
                    "feature_kind": "stop",
                    "name": stop.get("stop_name") or stop_id,
                    "stop_id": stop_id,
                    "direction_id": direction_id,
                },
            }
        )
    return {
        "priority": priority,
        "title": f"Route {route_id} direction {direction_id}: {label.lower()} by section",
        "description": "Colored lines show performance between consecutive scheduled stops; stop dots provide landmarks.",
        "geojson": {"type": "FeatureCollection", "features": features},
        "style": {**style, "target": "segment", "value_label": label},
        "popup_fields": [
            {"property": "from_stop_name", "label": "From"},
            {"property": "to_stop_name", "label": "To"},
            {"property": "metric_display", "label": label},
            {"property": "observations", "label": "Observed trips"},
        ],
    }


def route_pair_geometries(
    route: dict[str, Any],
    direction_id: int,
) -> dict[tuple[str, str], list[list[float]]]:
    trips = sorted(
        (
            trip
            for trip in (route.get("trips") or {}).values()
            if int(trip.get("direction_id") or 0) == direction_id
        ),
        key=lambda trip: len(trip.get("stop_times") or []),
        reverse=True,
    )
    shapes = route.get("shapes") or {}
    pairs: dict[tuple[str, str], list[list[float]]] = {}
    for trip in trips:
        shape = shapes.get(trip.get("shape_id")) or []
        stop_times = trip.get("stop_times") or []
        for previous, current in zip(stop_times, stop_times[1:]):
            key = (str(previous.get("stop_id")), str(current.get("stop_id")))
            if key in pairs:
                continue
            start_distance = float(previous.get("dist_along_route_m") or 0)
            end_distance = float(current.get("dist_along_route_m") or 0)
            coordinates = slice_shape_coordinates(shape, start_distance, end_distance, 20)
            if len(coordinates) >= 2:
                pairs[key] = coordinates
    return pairs


def sample_shape_coordinates(shape: list, maximum: int) -> list[list[float]]:
    coordinates = [[float(point[1]), float(point[0])] for point in shape if len(point) >= 2]
    return sample_coordinates(coordinates, maximum)


def sample_coordinates(coordinates: list[list[float]], maximum: int) -> list[list[float]]:
    if len(coordinates) <= maximum:
        return coordinates
    indices = [round(index * (len(coordinates) - 1) / (maximum - 1)) for index in range(maximum)]
    return [coordinates[index] for index in dict.fromkeys(indices)]


def slice_shape_coordinates(
    shape: list,
    start_distance: float,
    end_distance: float,
    maximum: int,
) -> list[list[float]]:
    if len(shape) < 2 or end_distance <= start_distance:
        return []
    coordinates = [coordinate_at_distance(shape, start_distance)]
    coordinates.extend(
        [float(point[1]), float(point[0])]
        for point in shape
        if len(point) >= 3 and start_distance < float(point[2]) < end_distance
    )
    coordinates.append(coordinate_at_distance(shape, end_distance))
    return sample_coordinates(coordinates, maximum)


def coordinate_at_distance(shape: list, target: float) -> list[float]:
    if target <= float(shape[0][2]):
        return [float(shape[0][1]), float(shape[0][0])]
    for previous, current in zip(shape, shape[1:]):
        previous_distance = float(previous[2])
        current_distance = float(current[2])
        if target > current_distance:
            continue
        span = current_distance - previous_distance
        ratio = 0.0 if span <= 0 else (target - previous_distance) / span
        lat = float(previous[0]) + (float(current[0]) - float(previous[0])) * ratio
        lon = float(previous[1]) + (float(current[1]) - float(previous[1])) * ratio
        return [lon, lat]
    return [float(shape[-1][1]), float(shape[-1][0])]


def map_style(metric: str, values: list[float]) -> dict[str, Any]:
    fixed = {
        "median_delay": [(0, "#2b8a3e", "On schedule"), (3, "#f59f00", "+3 min"), (7, "#c92a2a", "+7 min or more")],
        "p95_delay": [(0, "#2b8a3e", "On schedule"), (3, "#f59f00", "+3 min"), (7, "#c92a2a", "+7 min or more")],
        "delay_variability": [(0, "#2b8a3e", "Consistent"), (2, "#f59f00", "2 min std dev"), (5, "#c92a2a", "5+ min std dev")],
        "median_schedule_loss": [(-1, "#2b8a3e", "1 min faster"), (1, "#f59f00", "1 min added"), (5, "#c92a2a", "5+ min added")],
        "p95_schedule_loss": [(-1, "#2b8a3e", "1 min faster"), (2, "#f59f00", "2 min added"), (7, "#c92a2a", "7+ min added")],
        "median_speed": [(5, "#c92a2a", "5 mph"), (12, "#f59f00", "12 mph"), (20, "#2b8a3e", "20+ mph")],
    }
    if metric in fixed:
        stops = fixed[metric]
    else:
        ordered = sorted(values)
        low = ordered[0]
        middle = ordered[len(ordered) // 2]
        high = ordered[-1]
        if high <= low:
            high = low + 1
        middle = max(low + 0.01, min(middle, high - 0.01))
        stops = [
            (low, "#2c7fb8", format_map_value(low, "minutes")),
            (middle, "#fec44f", format_map_value(middle, "minutes")),
            (high, "#c51b7d", format_map_value(high, "minutes")),
        ]
    return {
        "property": "metric_value",
        "stops": [
            {"value": value, "color": color, "label": label}
            for value, color, label in stops
        ],
        "no_data_color": "#a8b2bd",
    }


def format_map_value(value: float, unit: str) -> str:
    rounded = round(float(value), 1)
    if unit == "mph":
        return f"{rounded:.1f} mph"
    if unit == "minutes standard deviation":
        return f"{rounded:.1f} min standard deviation"
    return f"{rounded:+.1f} min" if rounded < 0 else f"{rounded:.1f} min"


def spatial_bigquery_source(project_id: str, dates: list[str]) -> dict[str, Any]:
    return {
        "kind": "bigquery",
        "table": f"{project_id}.actransit.trip_observations",
        "start_date": dates[0],
        "end_date": dates[-1],
        "deduplicated": True,
    }


def processed_gtfs_source(route_id: str, route: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": "processed_gtfs",
        "route_id": route_id,
        "object": f"gtfs/processed/route_{route_id}.json",
        "feed_hash": route.get("feed_hash"),
    }


def validate_route_ids(route_ids: list[str], minimum: int, maximum: int) -> list[str]:
    unique = list(dict.fromkeys(validate_route_id(route_id) for route_id in route_ids))
    if len(unique) < minimum or len(unique) > maximum:
        raise ValueError(f"Provide between {minimum} and {maximum} distinct route IDs")
    return unique


def validate_metric(metric: str) -> None:
    if metric not in METRICS:
        raise ValueError(f"metric must be one of: {', '.join(METRICS)}")


def validate_date_range(start_date: str, end_date: str) -> None:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    if (end - start).days > 30:
        raise ValueError("The maximum analysis window is 31 calendar days")


def find_route(report: dict[str, Any], route_id: str) -> dict[str, Any] | None:
    return next((route for route in report.get("routes") or [] if str(route.get("route_id")).upper() == route_id), None)


def daily_source(dates: list[str]) -> dict[str, Any]:
    return {
        "kind": "daily_stats",
        "start_date": dates[0],
        "end_date": dates[-1],
        "object_count": len(dates),
    }


def tool_result(
    answer_context: str,
    timeframe: dict[str, Any],
    charts: list[dict[str, Any]],
    tables: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    warnings: list[str],
    maps: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "answer_context": answer_context,
        "timeframe": timeframe,
        "maps": (maps or [])[:3],
        "charts": charts[:3],
        "tables": [{**table, "rows": table.get("rows", [])[:250]} for table in tables[:2]],
        "sources": sources[:8],
        "warnings": warnings[:8],
    }


def active_services(body: bytes, service_date: str) -> set[str]:
    target = date.fromisoformat(service_date)
    yyyymmdd = target.strftime("%Y%m%d")
    weekday = target.strftime("%A").lower()
    active: set[str] = set()
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        if "calendar.txt" in archive.namelist():
            for row in csv.DictReader(io.TextIOWrapper(archive.open("calendar.txt"), encoding="utf-8-sig")):
                if row.get("start_date", "") <= yyyymmdd <= row.get("end_date", "") and row.get(weekday) == "1":
                    active.add(row["service_id"])
        if "calendar_dates.txt" in archive.namelist():
            for row in csv.DictReader(io.TextIOWrapper(archive.open("calendar_dates.txt"), encoding="utf-8-sig")):
                if row.get("date") != yyyymmdd:
                    continue
                if row.get("exception_type") == "1":
                    active.add(row["service_id"])
                elif row.get("exception_type") == "2":
                    active.discard(row["service_id"])
    return active


def route_trip_templates(body: bytes, route_id: str) -> dict[str, list[dict[str, Any]]]:
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        trip_services = {
            row["trip_id"]: row["service_id"]
            for row in csv.DictReader(io.TextIOWrapper(archive.open("trips.txt"), encoding="utf-8-sig"))
            if row.get("route_id", "").upper() == route_id
        }
        starts: dict[str, int] = {}
        for row in csv.DictReader(io.TextIOWrapper(archive.open("stop_times.txt"), encoding="utf-8-sig")):
            trip_id = row.get("trip_id") or ""
            if trip_id not in trip_services:
                continue
            start_seconds = gtfs_time_seconds(row.get("arrival_time") or row.get("departure_time") or "")
            if start_seconds is None:
                continue
            if trip_id not in starts or start_seconds < starts[trip_id]:
                starts[trip_id] = start_seconds

    templates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trip_id, start_seconds in starts.items():
        templates[trip_services[trip_id]].append(
            {"trip_id": trip_id, "start_seconds": start_seconds}
        )
    return templates


def gtfs_time_seconds(value: str) -> int | None:
    parts = value.split(":")
    if len(parts) != 3:
        return None
    try:
        hours, minutes, seconds = (int(part) for part in parts)
    except ValueError:
        return None
    if hours < 0 or not 0 <= minutes < 60 or not 0 <= seconds < 60:
        return None
    return hours * 3600 + minutes * 60 + seconds


def scheduled_rows(
    body: bytes,
    service_date: str,
    route_id: str,
    start_local: datetime,
    end_local: datetime,
    direction_id: int | None,
    stop_id: str | None,
    trip_id: str | None,
) -> list[dict[str, Any]]:
    services = active_services(body, service_date)
    base_date = date.fromisoformat(service_date)
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        trips: dict[str, dict[str, Any]] = {}
        for row in csv.DictReader(io.TextIOWrapper(archive.open("trips.txt"), encoding="utf-8-sig")):
            if row.get("service_id") not in services or row.get("route_id", "").upper() != route_id:
                continue
            direction = int(row.get("direction_id") or 0)
            if direction_id is not None and direction != direction_id:
                continue
            if trip_id and row.get("trip_id") != trip_id:
                continue
            trips[row["trip_id"]] = {"direction_id": direction}

        stops = {
            row["stop_id"]: row.get("stop_name") or row["stop_id"]
            for row in csv.DictReader(io.TextIOWrapper(archive.open("stops.txt"), encoding="utf-8-sig"))
        }
        rows = []
        for row in csv.DictReader(io.TextIOWrapper(archive.open("stop_times.txt"), encoding="utf-8-sig")):
            if row.get("trip_id") not in trips:
                continue
            if stop_id and row.get("stop_id") != stop_id:
                continue
            scheduled = gtfs_datetime(base_date, row.get("arrival_time") or "")
            if scheduled is None or not (start_local <= scheduled < end_local):
                continue
            rows.append(
                {
                    "trip_id": row["trip_id"],
                    "direction_id": trips[row["trip_id"]]["direction_id"],
                    "stop_sequence": int(row.get("stop_sequence") or 0),
                    "stop_id": row.get("stop_id"),
                    "stop_name": stops.get(row.get("stop_id", ""), row.get("stop_id")),
                    "scheduled_arrival": scheduled,
                }
            )
    rows.sort(key=lambda row: (row["scheduled_arrival"], row["trip_id"], row["stop_sequence"]))
    return rows


def gtfs_datetime(service_date: date, value: str) -> datetime | None:
    seconds = gtfs_time_seconds(value)
    if seconds is None:
        return None
    return datetime.combine(service_date, time(0, 0), PACIFIC) + timedelta(seconds=seconds)
