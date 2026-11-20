from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import math
import re
from typing import Any

from core import PACIFIC, default_analysis_window


MAX_QUERY_DAYS = 31
MAX_COLUMNS = 12
MAX_AGGREGATIONS = 5
MAX_FILTERS = 10
MAX_ORDER_BY = 3
MAX_FILTER_VALUES = 20
MAX_ROWS = 200
ALIAS_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,39}$")


@dataclass(frozen=True)
class ColumnSpec:
    type: str
    description: str


@dataclass(frozen=True)
class TableSpec:
    description: str
    columns: dict[str, ColumnSpec]
    deduplicate_by: tuple[str, ...] = ()
    deduplicate_order: str = ""


TABLES = {
    "trip_observations": TableSpec(
        description=(
            "Deduplicated scheduled-versus-observed stop arrivals and stop-to-stop metrics. "
            "One logical row per service date, trip, and stop sequence."
        ),
        columns={
            "service_date": ColumnSpec("DATE", "Pacific service date."),
            "route_id": ColumnSpec("STRING", "Public route identifier."),
            "direction_id": ColumnSpec("INT64", "GTFS direction identifier, normally 0 or 1."),
            "trip_id": ColumnSpec("STRING", "GTFS trip identifier."),
            "vehicle_id": ColumnSpec("STRING", "Realtime vehicle identifier."),
            "stop_sequence": ColumnSpec("INT64", "Stop order within the trip."),
            "stop_id": ColumnSpec("STRING", "GTFS stop identifier."),
            "scheduled_arrival": ColumnSpec("TIMESTAMP", "Scheduled stop arrival timestamp."),
            "actual_arrival": ColumnSpec("TIMESTAMP", "Observed arrival, null when not observed."),
            "delay_seconds": ColumnSpec("INT64", "Actual minus scheduled arrival in seconds."),
            "scheduled_departure": ColumnSpec("TIMESTAMP", "Scheduled origin departure timestamp."),
            "actual_departure": ColumnSpec("TIMESTAMP", "Observed or estimated origin departure timestamp."),
            "departure_delay_seconds": ColumnSpec("INT64", "Actual minus scheduled origin departure in seconds."),
            "departure_confidence": ColumnSpec("STRING", "Origin departure detection confidence."),
            "leg_distance_m": ColumnSpec("FLOAT64", "Distance from the preceding stop in meters."),
            "leg_duration_s": ColumnSpec("FLOAT64", "Observed time from the preceding stop in seconds."),
            "leg_avg_speed_mps": ColumnSpec("FLOAT64", "Observed average speed from the preceding stop."),
            "is_stale": ColumnSpec("BOOL", "Whether tracking ended through the stale-trip timeout."),
            "ingested_at": ColumnSpec("TIMESTAMP", "Warehouse ingestion timestamp."),
        },
        deduplicate_by=("service_date", "trip_id", "stop_sequence"),
        deduplicate_order="IF(actual_arrival IS NULL, 1, 0), ingested_at DESC",
    ),
    "trip_probes": TableSpec(
        description=(
            "Deduplicated historical vehicle probes projected onto a route. "
            "Use bounded raw samples or aggregates rather than full-trip exports."
        ),
        columns={
            "service_date": ColumnSpec("DATE", "Pacific service date."),
            "route_id": ColumnSpec("STRING", "Public route identifier."),
            "trip_id": ColumnSpec("STRING", "GTFS trip identifier."),
            "vehicle_id": ColumnSpec("STRING", "Realtime vehicle identifier."),
            "observed_at": ColumnSpec("TIMESTAMP", "Probe observation timestamp."),
            "lat": ColumnSpec("FLOAT64", "Observed latitude."),
            "lon": ColumnSpec("FLOAT64", "Observed longitude."),
            "bearing_deg": ColumnSpec("FLOAT64", "Reported bearing in degrees."),
            "reported_speed_mps": ColumnSpec("FLOAT64", "Reported speed in meters per second."),
            "dist_along_route_m": ColumnSpec("FLOAT64", "Projected distance along the route shape."),
            "nearest_stop_seq": ColumnSpec("INT64", "Nearest scheduled stop sequence."),
            "ingested_at": ColumnSpec("TIMESTAMP", "Warehouse ingestion timestamp."),
        },
        deduplicate_by=("service_date", "trip_id", "observed_at"),
        deduplicate_order="ingested_at DESC",
    ),
    "ridership_observations": TableSpec(
        description=(
            "Minute-level realtime APC and crowding attributes. Direct passenger counts are nullable; "
            "status-band estimates are separate fields."
        ),
        columns={
            "service_date": ColumnSpec("DATE", "Pacific service date."),
            "observed_at": ColumnSpec("TIMESTAMP", "Observation timestamp."),
            "vehicle_id": ColumnSpec("STRING", "Realtime vehicle identifier."),
            "route_id": ColumnSpec("STRING", "Public route identifier, when supplied."),
            "trip_id": ColumnSpec("STRING", "GTFS trip identifier, when supplied."),
            "latitude": ColumnSpec("FLOAT64", "Observed latitude."),
            "longitude": ColumnSpec("FLOAT64", "Observed longitude."),
            "position_reported_at": ColumnSpec("TIMESTAMP", "Upstream position timestamp."),
            "vehicle_capacity": ColumnSpec("INT64", "Published vehicle capacity."),
            "current_passenger_count": ColumnSpec("INT64", "Direct APC passenger count, usually null."),
            "estimated_occupancy_percentage": ColumnSpec("INT64", "Upstream occupancy percentage, usually null."),
            "estimated_occupancy_status": ColumnSpec("STRING", "Published or estimated crowding status band."),
            "estimated_occupancy_status_color": ColumnSpec("STRING", "Display color associated with the status band."),
            "apc_reported_at": ColumnSpec("TIMESTAMP", "Upstream APC report timestamp."),
            "ingested_at": ColumnSpec("TIMESTAMP", "Warehouse ingestion timestamp."),
        },
    ),
    "api_request_observations": TableSpec(
        description=(
            "One bounded health observation per scheduled upstream API request. "
            "Raw internal error-message text is intentionally not exposed."
        ),
        columns={
            "service_date": ColumnSpec("DATE", "Pacific service date."),
            "observed_at": ColumnSpec("TIMESTAMP", "Request observation timestamp."),
            "source": ColumnSpec("STRING", "Upstream feed source."),
            "endpoint": ColumnSpec("STRING", "Normalized upstream endpoint."),
            "latency_ms": ColumnSpec("FLOAT64", "Request latency in milliseconds."),
            "status_code": ColumnSpec("INT64", "HTTP status code, when available."),
            "success": ColumnSpec("BOOL", "Whether the request succeeded."),
            "outcome": ColumnSpec("STRING", "Bounded request outcome category."),
            "response_bytes": ColumnSpec("INT64", "Response size in bytes, when available."),
            "ingested_at": ColumnSpec("TIMESTAMP", "Warehouse ingestion timestamp."),
        },
    ),
    "api_request_hourly": TableSpec(
        description="Permanent hourly and daily upstream API request-health aggregates.",
        columns={
            "service_date": ColumnSpec("DATE", "Pacific service date."),
            "source": ColumnSpec("STRING", "Upstream feed source."),
            "is_total": ColumnSpec("BOOL", "True for the daily total row."),
            "hour_start": ColumnSpec("TIMESTAMP", "Start of the aggregate hour; null for daily totals."),
            "requests": ColumnSpec("INT64", "Total requests."),
            "successful_requests": ColumnSpec("INT64", "Successful requests."),
            "p50_latency_ms": ColumnSpec("FLOAT64", "Median latency in milliseconds."),
            "p95_latency_ms": ColumnSpec("FLOAT64", "95th-percentile latency in milliseconds."),
            "p99_latency_ms": ColumnSpec("FLOAT64", "99th-percentile latency in milliseconds."),
            "max_latency_ms": ColumnSpec("FLOAT64", "Maximum latency in milliseconds."),
            "timeout_count": ColumnSpec("INT64", "Timed-out requests."),
            "http_4xx_count": ColumnSpec("INT64", "HTTP 4xx responses."),
            "http_5xx_count": ColumnSpec("INT64", "HTTP 5xx responses."),
            "other_error_count": ColumnSpec("INT64", "Other failed requests."),
            "updated_at": ColumnSpec("TIMESTAMP", "Aggregate update timestamp."),
        },
    ),
}

FILTER_OPERATORS = {
    "eq",
    "ne",
    "lt",
    "lte",
    "gt",
    "gte",
    "in",
    "not_in",
    "contains",
    "is_null",
    "is_not_null",
}
AGGREGATION_OPERATIONS = {
    "count",
    "count_distinct",
    "sum",
    "avg",
    "min",
    "max",
    "median",
    "p95",
    "stddev",
}
NUMERIC_TYPES = {"INT64", "FLOAT64"}


@dataclass(frozen=True)
class QueryParameter:
    name: str
    type: str
    value: Any
    array: bool = False


@dataclass(frozen=True)
class CompiledQuery:
    sql: str
    parameters: list[QueryParameter]
    table: str
    columns: list[str]
    aggregations: list[dict[str, Any]]
    filters: list[dict[str, Any]]
    group_by: list[str]
    order_by: list[dict[str, Any]]
    result_columns: list[str]
    limit: int
    deduplicated: bool


@dataclass(frozen=True)
class QueryDates:
    start_date: str
    end_date: str
    timeframe: dict[str, Any]


def describe_schema(table: str | None = None) -> dict[str, Any]:
    if table is not None and table not in TABLES:
        raise ValueError(f"table must be one of: {', '.join(TABLES)}")
    names = [table] if table else list(TABLES)
    tables = []
    for name in names:
        spec = TABLES[name]
        tables.append(
            {
                "name": name,
                "description": spec.description,
                "deduplicated": bool(spec.deduplicate_by),
                "columns": [
                    {
                        "name": column_name,
                        "type": column.type,
                        "description": column.description,
                    }
                    for column_name, column in spec.columns.items()
                ],
            }
        )
    return {
        "tables": tables,
        "query_contract": {
            "date_partition_required": True,
            "maximum_days": MAX_QUERY_DAYS,
            "maximum_rows": MAX_ROWS,
            "filter_operators": sorted(FILTER_OPERATORS),
            "aggregation_operations": sorted(AGGREGATION_OPERATIONS),
            "visualizations": ["line", "bar", "pie", "heatmap", "map"],
            "raw_sql_accepted": False,
        },
    }


def resolve_query_dates(
    start_date: str | None,
    end_date: str | None,
    today: date | None = None,
) -> QueryDates:
    assumed = start_date is None and end_date is None
    if assumed:
        start_date, end_date = default_analysis_window(today)
        basis = "seven_day_period_ending_yesterday"
        disclosure = (
            "No timeframe was provided, so this exploratory query uses the seven-day period "
            f"ending yesterday: {start_date} through {end_date}."
        )
    else:
        start_date = start_date or end_date
        end_date = end_date or start_date
        basis = "user_supplied_or_interpreted_dates"
        disclosure = f"Queried service dates from {start_date} through {end_date}."
    try:
        start = date.fromisoformat(str(start_date))
        end = date.fromisoformat(str(end_date))
    except ValueError as exc:
        raise ValueError("start_date and end_date must use YYYY-MM-DD") from exc
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    if (end - start).days >= MAX_QUERY_DAYS:
        raise ValueError(f"Structured queries are limited to {MAX_QUERY_DAYS} calendar days")
    current = today or datetime.now(PACIFIC).date()
    if end >= current:
        raise ValueError("Structured queries are limited to completed service dates before today")
    return QueryDates(
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        timeframe={
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "assumed": assumed,
            "basis": basis,
            "disclosure": disclosure,
        },
    )


def compile_query(
    project_id: str,
    table: str,
    start_date: str,
    end_date: str,
    columns: list[str] | None = None,
    aggregations: list[dict[str, Any]] | None = None,
    filters: list[dict[str, Any]] | None = None,
    group_by: list[str] | None = None,
    order_by: list[dict[str, Any]] | None = None,
    limit: int = 50,
) -> CompiledQuery:
    if table not in TABLES:
        raise ValueError(f"table must be one of: {', '.join(TABLES)}")
    spec = TABLES[table]
    columns = unique_list(columns or [], "columns", MAX_COLUMNS)
    aggregations = [plain_dict(value) for value in (aggregations or [])]
    filters = [plain_dict(value) for value in (filters or [])]
    group_by = unique_list(group_by or [], "group_by", MAX_COLUMNS)
    order_by = [plain_dict(value) for value in (order_by or [])]
    if len(aggregations) > MAX_AGGREGATIONS:
        raise ValueError(f"At most {MAX_AGGREGATIONS} aggregations are allowed")
    if len(filters) > MAX_FILTERS:
        raise ValueError(f"At most {MAX_FILTERS} filters are allowed")
    if len(order_by) > MAX_ORDER_BY:
        raise ValueError(f"At most {MAX_ORDER_BY} order fields are allowed")
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= MAX_ROWS:
        raise ValueError(f"limit must be between 1 and {MAX_ROWS}")

    if not columns and group_by:
        columns = list(group_by)
    for column in [*columns, *group_by]:
        validate_column(spec, column)
    if aggregations and columns and not group_by:
        group_by = list(columns)
    if group_by and set(group_by) != set(columns):
        raise ValueError("group_by must contain exactly the selected non-aggregate columns")
    if not columns and not aggregations:
        raise ValueError("Select at least one column or aggregation")

    select_parts = [quoted(column) for column in columns]
    normalized_aggregations = []
    aliases = set(columns)
    required_source_columns = set(columns)
    for aggregation in aggregations:
        normalized = normalize_aggregation(spec, aggregation, aliases)
        normalized_aggregations.append(normalized)
        aliases.add(normalized["alias"])
        if normalized.get("column"):
            required_source_columns.add(normalized["column"])
        select_parts.append(aggregation_sql(normalized))

    where_parts = []
    parameters = [
        QueryParameter("start_date", "DATE", date.fromisoformat(start_date)),
        QueryParameter("end_date", "DATE", date.fromisoformat(end_date)),
    ]
    normalized_filters = []
    for index, query_filter in enumerate(filters):
        normalized, predicate, parameter = normalize_filter(spec, query_filter, index)
        normalized_filters.append(normalized)
        required_source_columns.add(normalized["column"])
        where_parts.append(predicate)
        if parameter:
            parameters.append(parameter)

    result_columns = [*columns, *[item["alias"] for item in normalized_aggregations]]
    normalized_order = []
    for value in order_by:
        column = str(value.get("column") or "")
        direction = str(value.get("direction") or "asc").lower()
        if column not in result_columns:
            raise ValueError(f"order_by column must be one of: {', '.join(result_columns)}")
        if direction not in {"asc", "desc"}:
            raise ValueError("order_by direction must be asc or desc")
        normalized_order.append({"column": column, "direction": direction})

    required_source_columns.add("service_date")
    source_columns = ", ".join(quoted(value) for value in sorted(required_source_columns))
    dedupe = ""
    if spec.deduplicate_by:
        partition = ", ".join(quoted(value) for value in spec.deduplicate_by)
        dedupe = (
            "\n  QUALIFY ROW_NUMBER() OVER ("
            f"PARTITION BY {partition} ORDER BY {spec.deduplicate_order}"
            ") = 1"
        )
    sql = (
        "WITH bounded_rows AS (\n"
        f"  SELECT {source_columns}\n"
        f"  FROM `{project_id}.actransit.{table}`\n"
        "  WHERE service_date BETWEEN @start_date AND @end_date"
        f"{dedupe}\n"
        ")\n"
        f"SELECT {', '.join(select_parts)}\n"
        "FROM bounded_rows"
    )
    if where_parts:
        sql += "\nWHERE " + " AND ".join(where_parts)
    if group_by:
        sql += "\nGROUP BY " + ", ".join(quoted(value) for value in group_by)
    if normalized_order:
        sql += "\nORDER BY " + ", ".join(
            f"{quoted(value['column'])} {value['direction'].upper()}" for value in normalized_order
        )
    sql += f"\nLIMIT {limit}"
    return CompiledQuery(
        sql=sql,
        parameters=parameters,
        table=table,
        columns=columns,
        aggregations=normalized_aggregations,
        filters=normalized_filters,
        group_by=group_by,
        order_by=normalized_order,
        result_columns=result_columns,
        limit=limit,
        deduplicated=bool(spec.deduplicate_by),
    )


def build_artifacts(
    table: str,
    rows: list[dict[str, Any]],
    result_columns: list[str],
    visualization: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    if not visualization:
        return [], [], []
    visualization = validate_visualization(result_columns, visualization)
    if not rows:
        return [], [], []
    title = f"Exploratory {table.replace('_', ' ')} query"
    kind = str(visualization.get("type") or "").lower()
    chart_title = str(visualization.get("title") or title).strip()[:120]
    if kind == "map":
        return [], [build_point_map(rows, result_columns, visualization, chart_title)], []
    if kind == "heatmap":
        return [build_heatmap(rows, result_columns, visualization, chart_title)], [], []

    x_column = require_result_column(visualization.get("x_column"), result_columns, "x_column")
    value_columns = list(visualization.get("value_columns") or [])
    if kind == "pie" and len(value_columns) != 1:
        raise ValueError("pie charts require exactly one value column")
    if not 1 <= len(value_columns) <= 5:
        raise ValueError("line and bar charts require one to five value columns")
    for column in value_columns:
        require_result_column(column, result_columns, "value_columns")
    labels = [display_value(row.get(x_column)) for row in rows]
    series = [
        {
            "name": humanize(column),
            "values": [numeric_or_none(row.get(column)) for row in rows],
        }
        for column in value_columns
    ]
    return [
        {
            "type": kind,
            "title": chart_title,
            "x_label": str(visualization.get("x_label") or humanize(x_column)),
            "y_label": str(visualization.get("y_label") or ""),
            "labels": labels,
            "series": series,
        }
    ], [], []


def validate_visualization(
    result_columns: list[str],
    visualization: dict[str, Any],
) -> dict[str, Any]:
    normalized = plain_dict(visualization)
    kind = str(normalized.get("type") or "").lower()
    if kind not in {"line", "bar", "pie", "heatmap", "map"}:
        raise ValueError("visualization type must be line, bar, pie, heatmap, or map")
    values = list(normalized.get("value_columns") or [])
    if kind == "map":
        require_result_column(normalized.get("latitude_column"), result_columns, "latitude_column")
        require_result_column(normalized.get("longitude_column"), result_columns, "longitude_column")
        if normalized.get("label_column") is not None:
            require_result_column(normalized["label_column"], result_columns, "label_column")
        if len(values) > 1:
            raise ValueError("maps accept at most one value column")
    elif kind == "heatmap":
        require_result_column(normalized.get("x_column"), result_columns, "x_column")
        require_result_column(normalized.get("y_column"), result_columns, "y_column")
        if len(values) != 1:
            raise ValueError("heatmaps require exactly one value column")
    else:
        require_result_column(normalized.get("x_column"), result_columns, "x_column")
        if kind == "pie" and len(values) != 1:
            raise ValueError("pie charts require exactly one value column")
        if not 1 <= len(values) <= 5:
            raise ValueError("line and bar charts require one to five value columns")
    for column in values:
        require_result_column(column, result_columns, "value_columns")
    return normalized


def build_heatmap(
    rows: list[dict[str, Any]],
    result_columns: list[str],
    visualization: dict[str, Any],
    title: str,
) -> dict[str, Any]:
    x_column = require_result_column(visualization.get("x_column"), result_columns, "x_column")
    y_column = require_result_column(visualization.get("y_column"), result_columns, "y_column")
    values = list(visualization.get("value_columns") or [])
    if len(values) != 1:
        raise ValueError("heatmaps require exactly one value column")
    value_column = require_result_column(values[0], result_columns, "value_columns")
    x_labels = unique_display_values(rows, x_column)
    y_labels = unique_display_values(rows, y_column)
    if len(x_labels) > 24 or len(y_labels) > 7:
        raise ValueError("heatmaps are limited to 24 x-axis labels and 7 y-axis labels")
    numeric_values = [numeric_or_none(row.get(value_column)) for row in rows]
    usable = [value for value in numeric_values if value is not None]
    cells = [
        {
            "x": display_value(row.get(x_column)),
            "y": display_value(row.get(y_column)),
            "value": numeric_or_none(row.get(value_column)),
            "display": display_value(row.get(value_column)),
        }
        for row in rows
    ]
    return {
        "type": "heatmap",
        "title": title,
        "x_labels": x_labels,
        "y_labels": y_labels,
        "x_label": str(visualization.get("x_label") or humanize(x_column)),
        "y_label": str(visualization.get("y_label") or humanize(y_column)),
        "value_label": str(visualization.get("value_label") or humanize(value_column)),
        "low_label": str(visualization.get("low_label") or "Lower"),
        "high_label": str(visualization.get("high_label") or "Higher"),
        "min_value": min(usable) if usable else 0,
        "max_value": max(usable) if usable else 1,
        "cells": cells,
    }


def build_point_map(
    rows: list[dict[str, Any]],
    result_columns: list[str],
    visualization: dict[str, Any],
    title: str,
) -> dict[str, Any]:
    latitude = require_result_column(
        visualization.get("latitude_column"), result_columns, "latitude_column"
    )
    longitude = require_result_column(
        visualization.get("longitude_column"), result_columns, "longitude_column"
    )
    label_column = visualization.get("label_column")
    if label_column is not None:
        label_column = require_result_column(label_column, result_columns, "label_column")
    value_columns = list(visualization.get("value_columns") or [])
    if len(value_columns) > 1:
        raise ValueError("maps accept at most one value column")
    value_column = None
    if value_columns:
        value_column = require_result_column(value_columns[0], result_columns, "value_columns")

    features = []
    metric_values = []
    for index, row in enumerate(rows):
        lat = numeric_or_none(row.get(latitude))
        lon = numeric_or_none(row.get(longitude))
        if lat is None or lon is None or not -90 <= lat <= 90 or not -180 <= lon <= 180:
            continue
        metric = numeric_or_none(row.get(value_column)) if value_column else None
        if metric is not None:
            metric_values.append(metric)
        label = display_value(row.get(label_column)) if label_column else f"Point {index + 1}"
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "feature_kind": "stop",
                    "label": label,
                    "metric_value": metric,
                    "metric_display": display_value(row.get(value_column)) if value_column else "",
                    "has_metric": metric is not None,
                },
            }
        )
    if not features:
        raise ValueError("The selected latitude and longitude columns returned no mappable points")
    style = {
        "target": "stop",
        "property": "metric_value",
        "value_label": humanize(value_column) if value_column else "Locations",
        "stops": numeric_color_stops(metric_values),
        "no_data_color": "#2c7fb8",
    }
    popup_fields = [{"property": "label", "label": humanize(label_column) if label_column else "Point"}]
    if value_column:
        popup_fields.append({"property": "metric_display", "label": humanize(value_column)})
    return {
        "title": title,
        "description": f"{len(features)} bounded query-result locations.",
        "geojson": {"type": "FeatureCollection", "features": features},
        "style": style,
        "popup_fields": popup_fields,
    }


def normalize_aggregation(
    table: TableSpec,
    aggregation: dict[str, Any],
    aliases: set[str],
) -> dict[str, Any]:
    operation = str(aggregation.get("operation") or "").lower()
    if operation not in AGGREGATION_OPERATIONS:
        raise ValueError(
            f"aggregation operation must be one of: {', '.join(sorted(AGGREGATION_OPERATIONS))}"
        )
    column = aggregation.get("column")
    if column is not None:
        column = str(column)
        validate_column(table, column)
    if operation != "count" and not column:
        raise ValueError(f"{operation} requires a column")
    if operation in {"sum", "avg", "median", "p95", "stddev"}:
        if table.columns[column].type not in NUMERIC_TYPES:
            raise ValueError(f"{operation} requires a numeric column")
    alias = str(aggregation.get("alias") or "")
    if not ALIAS_PATTERN.fullmatch(alias):
        raise ValueError("aggregation aliases must start with a letter and contain only lowercase letters, digits, and underscores")
    if alias in aliases:
        raise ValueError(f"Duplicate result column: {alias}")
    return {"operation": operation, "column": column, "alias": alias}


def aggregation_sql(aggregation: dict[str, Any]) -> str:
    operation = aggregation["operation"]
    column = aggregation.get("column")
    operand = quoted(column) if column else "*"
    expressions = {
        "count": f"COUNT({operand})",
        "count_distinct": f"COUNT(DISTINCT {operand})",
        "sum": f"SUM({operand})",
        "avg": f"AVG({operand})",
        "min": f"MIN({operand})",
        "max": f"MAX({operand})",
        "median": f"APPROX_QUANTILES({operand}, 100)[OFFSET(50)]",
        "p95": f"APPROX_QUANTILES({operand}, 100)[OFFSET(95)]",
        "stddev": f"STDDEV(CAST({operand} AS FLOAT64))",
    }
    return f"{expressions[operation]} AS {quoted(aggregation['alias'])}"


def normalize_filter(
    table: TableSpec,
    query_filter: dict[str, Any],
    index: int,
) -> tuple[dict[str, Any], str, QueryParameter | None]:
    column = str(query_filter.get("column") or "")
    validate_column(table, column)
    if column == "service_date":
        raise ValueError("Use start_date and end_date instead of a service_date filter")
    operator = str(query_filter.get("operator") or "eq").lower()
    if operator not in FILTER_OPERATORS:
        raise ValueError(f"filter operator must be one of: {', '.join(sorted(FILTER_OPERATORS))}")
    column_type = table.columns[column].type
    value = query_filter.get("value")
    if operator in {"is_null", "is_not_null"}:
        suffix = "IS NULL" if operator == "is_null" else "IS NOT NULL"
        return {"column": column, "operator": operator}, f"{quoted(column)} {suffix}", None
    if operator == "contains" and column_type != "STRING":
        raise ValueError("contains is only available for STRING columns")
    parameter_name = f"filter_{index}"
    if operator in {"in", "not_in"}:
        if not isinstance(value, list) or not 1 <= len(value) <= MAX_FILTER_VALUES:
            raise ValueError(f"{operator} requires one to {MAX_FILTER_VALUES} values")
        coerced = [coerce_value(item, column_type) for item in value]
        comparison = "IN" if operator == "in" else "NOT IN"
        normalized_value = [serializable_value(item) for item in coerced]
        return (
            {"column": column, "operator": operator, "value": normalized_value},
            f"{quoted(column)} {comparison} UNNEST(@{parameter_name})",
            QueryParameter(parameter_name, column_type, coerced, array=True),
        )
    coerced = coerce_value(value, column_type)
    if operator == "contains":
        predicate = f"CONTAINS_SUBSTR({quoted(column)}, @{parameter_name})"
    else:
        comparison = {
            "eq": "=",
            "ne": "!=",
            "lt": "<",
            "lte": "<=",
            "gt": ">",
            "gte": ">=",
        }[operator]
        predicate = f"{quoted(column)} {comparison} @{parameter_name}"
    return (
        {"column": column, "operator": operator, "value": serializable_value(coerced)},
        predicate,
        QueryParameter(parameter_name, column_type, coerced),
    )


def coerce_value(value: Any, column_type: str) -> Any:
    if value is None:
        raise ValueError("filter value is required for this operator")
    try:
        if column_type == "STRING":
            if not isinstance(value, str) or not 1 <= len(value) <= 100:
                raise ValueError("string filter values must contain 1 to 100 characters")
            return value
        if column_type == "BOOL":
            if not isinstance(value, bool):
                raise ValueError("boolean columns require true or false")
            return value
        if column_type == "INT64":
            if isinstance(value, bool):
                raise ValueError("integer columns require an integer")
            converted = int(value)
            if float(value) != converted:
                raise ValueError("integer columns require an integer")
            return converted
        if column_type == "FLOAT64":
            if isinstance(value, bool):
                raise ValueError("numeric columns require a number")
            converted = float(value)
            if not math.isfinite(converted):
                raise ValueError("numeric filters must be finite")
            return converted
        if column_type == "DATE":
            return date.fromisoformat(str(value))
        if column_type == "TIMESTAMP":
            converted = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return converted if converted.tzinfo else converted.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError) as exc:
        if isinstance(exc, ValueError) and str(exc).startswith(("string ", "boolean ", "integer ", "numeric ")):
            raise
        raise ValueError(f"Invalid {column_type} filter value") from exc
    raise ValueError(f"Unsupported filter type: {column_type}")


def validate_column(table: TableSpec, column: str) -> None:
    if column not in table.columns:
        raise ValueError(f"column must be one of: {', '.join(table.columns)}")


def unique_list(values: list[Any], label: str, maximum: int) -> list[str]:
    normalized = [str(value) for value in values]
    if len(normalized) > maximum:
        raise ValueError(f"At most {maximum} {label} are allowed")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{label} cannot contain duplicates")
    return normalized


def plain_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(exclude_none=True)
    raise ValueError("Query specifications must be objects")


def quoted(value: str) -> str:
    return f"`{value}`"


def serializable_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: normalize_json_value(value) for key, value in row.items()}


def normalize_json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if hasattr(value, "as_tuple"):
        return float(value)
    return value


def require_result_column(value: Any, columns: list[str], label: str) -> str:
    column = str(value or "")
    if column not in columns:
        raise ValueError(f"{label} must be one of the query result columns: {', '.join(columns)}")
    return column


def unique_display_values(rows: list[dict[str, Any]], column: str) -> list[str]:
    values = []
    seen = set()
    for row in rows:
        value = display_value(row.get(column))
        if value not in seen:
            seen.add(value)
            values.append(value)
    return values


def numeric_or_none(value: Any) -> float | int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(converted):
        return None
    return int(converted) if converted.is_integer() else converted


def display_value(value: Any) -> str:
    if value is None:
        return "No data"
    if isinstance(value, float):
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return str(value)


def humanize(value: str | None) -> str:
    if not value:
        return "Value"
    return value.replace("_", " ").strip().title()


def numeric_color_stops(values: list[float]) -> list[dict[str, Any]]:
    if not values:
        return []
    ordered = sorted(values)
    low = ordered[0]
    middle = ordered[len(ordered) // 2]
    high = ordered[-1]
    if high <= low:
        high = low + 1
    middle = max(low + 0.01, min(middle, high - 0.01))
    return [
        {"value": low, "color": "#2c7fb8", "label": display_value(low)},
        {"value": middle, "color": "#fec44f", "label": display_value(middle)},
        {"value": high, "color": "#c51b7d", "label": display_value(high)},
    ]
