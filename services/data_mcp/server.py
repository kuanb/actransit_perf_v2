from __future__ import annotations

import os
from typing import Any, Literal

from fastmcp import FastMCP
from pydantic import BaseModel, Field
from starlette.responses import JSONResponse

from core import METRICS
from gcp_data import PROFILE_METRICS, TransitData


mcp = FastMCP(
    "AC Transit performance data",
    instructions=(
        "Read-only, bounded tools for published AC Transit performance reports and observations. "
        "When dates are omitted, tools use the four-week period ending yesterday in America/Los_Angeles. "
        "Route comparisons are service-group aware: regular Local/Rapid/Tempo, 600-series Supplementary "
        "School Service, 800-series All Nighter, and lettered Transbay routes must be analyzed separately. "
        "Prefer purpose-built tools. Use the structured query fallback only for supported questions those tools "
        "cannot answer; it accepts allowlisted fields and operations but never SQL."
    ),
)
data = TransitData()


class QueryFilter(BaseModel):
    column: str = Field(description="Exact allowlisted column name from describe_data.")
    operator: Literal[
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
    ] = "eq"
    value: str | int | float | bool | list[str | int | float | bool] | None = None


class QueryAggregation(BaseModel):
    operation: Literal[
        "count",
        "count_distinct",
        "sum",
        "avg",
        "min",
        "max",
        "median",
        "p95",
        "stddev",
    ]
    column: str | None = Field(
        default=None,
        description="Exact allowlisted source column. Only count may omit it to count rows.",
    )
    alias: str = Field(
        description="Lowercase result name starting with a letter and containing only letters, digits, or underscores."
    )


class QueryOrder(BaseModel):
    column: str = Field(description="Selected column or aggregation alias.")
    direction: Literal["asc", "desc"] = "asc"


class QueryVisualization(BaseModel):
    type: Literal["line", "bar", "pie", "heatmap", "map"]
    title: str | None = None
    x_column: str | None = Field(default=None, description="Result column used for chart labels or heatmap x values.")
    y_column: str | None = Field(default=None, description="Result column used for heatmap y values.")
    value_columns: list[str] = Field(
        default_factory=list,
        description="One to five numeric result columns; pie, heatmap, and map accept at most one.",
    )
    latitude_column: str | None = None
    longitude_column: str | None = None
    label_column: str | None = None
    x_label: str | None = None
    y_label: str | None = None
    value_label: str | None = None
    low_label: str | None = None
    high_label: str | None = None


def model_value(value: BaseModel | dict[str, Any]) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return value.model_dump(exclude_none=True)


def model_values(values: list[BaseModel | dict[str, Any]]) -> list[dict[str, Any]]:
    return [model_value(value) for value in values]


@mcp.tool
def compare_routes(
    route_ids: list[str],
    metric: Literal[
        "on_time_service_delivered",
        "trip_delivery",
        "on_time_within_3_minutes",
        "median_delay",
        "p95_delay",
        "average_speed",
        "dropped_trips",
        "bunched_headways",
        "long_gaps",
    ],
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """Compare two to five routes from the same service group using one approved metric and at most 31 calendar days. Mixed regular, 600-series school, 800-series All Nighter, and Transbay comparisons are rejected. Dates default to four weeks ending yesterday."""
    return data.compare_routes(route_ids, metric, start_date, end_date)


@mcp.tool
def get_route_performance(
    route_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """Summarize one route across rider-window, trip-delivery, delay, speed, bunching, and gap metrics. Dates default to four weeks ending yesterday."""
    return data.get_route_performance(route_id, start_date, end_date)


@mcp.tool
def get_route_delay_map(
    route_id: str,
    metric: Literal["median_delay", "p95_delay", "delay_variability"] = "median_delay",
    start_date: str | None = None,
    end_date: str | None = None,
    direction_id: int | None = None,
) -> dict:
    """Return one styled GeoJSON map per route direction, named by its representative current GTFS endpoints and marked with start/end terminals, with stops colored by median delay, 95th-percentile delay, or delay variability. Use for questions about where buses fall behind schedule or where reliability changes along a route. Dates default to four weeks ending yesterday and are limited to 31 calendar days."""
    return data.get_route_delay_map(route_id, metric, start_date, end_date, direction_id)


@mcp.tool
def get_route_segment_performance_map(
    route_id: str,
    metric: Literal[
        "median_schedule_loss",
        "p95_schedule_loss",
        "median_travel_time",
        "p95_travel_time",
        "median_speed",
    ] = "median_schedule_loss",
    start_date: str | None = None,
    end_date: str | None = None,
    direction_id: int | None = None,
) -> dict:
    """Return stop-to-stop route performance with styled GeoJSON and distance-profile charts. Median schedule loss returns one focused map for the greatest slowdown and one for the greatest catch-up; negative schedule change means catch-up, so do not substitute travel time for catch-up. Other metrics return one map per direction. The data cannot isolate a particular turn or intersection. Dates default to four weeks ending yesterday and are limited to 31 calendar days."""
    return data.get_route_segment_performance_map(
        route_id,
        metric,
        start_date,
        end_date,
        direction_id,
    )


@mcp.tool
def get_route_service_profile(
    route_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """Return daily absolute scheduled and Dropped / not observed trip counts plus a 24-hour by 7-day heatmap of dropped trips grouped by scheduled start time. Use this for questions about how many trips did not run by day or which hours and weekdays have the worst trip delivery. Dates default to four weeks ending yesterday and are limited to 31 calendar days."""
    return data.get_route_service_profile(route_id, start_date, end_date)


@mcp.tool
def get_system_performance(
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """Summarize systemwide service delivery, delay, speed, bunching, and gap metrics. Dates default to four weeks ending yesterday."""
    return data.get_system_performance(start_date, end_date)


@mcp.tool
def rank_routes(
    metric: Literal[
        "on_time_service_delivered",
        "trip_delivery",
        "on_time_within_3_minutes",
        "median_delay",
        "p95_delay",
        "average_speed",
        "dropped_trips",
        "bunched_headways",
        "long_gaps",
    ],
    start_date: str | None = None,
    end_date: str | None = None,
    order: Literal["worst", "best"] = "worst",
    limit: int = 10,
    route_group: Literal["regular", "school", "all_nighter", "transbay"] = "regular",
) -> dict:
    """Rank at most ten routes within one service group using an approved metric. Defaults to regular Local/Rapid/Tempo routes; 600-series school, 800-series All Nighter, and Transbay routes require their own separate ranking. Dates default to four weeks ending yesterday."""
    return data.rank_routes(metric, start_date, end_date, order, limit, route_group)


@mcp.tool
def rank_route_trends(
    metric: Literal["dropped_trips"],
    start_date: str | None = None,
    end_date: str | None = None,
    period: Literal["day", "week"] = "week",
    order: Literal["worsened", "improved"] = "worsened",
    limit: int = 10,
    route_group: Literal["regular", "school", "all_nighter", "transbay"] = "regular",
) -> dict:
    """Rank route changes across daily or consecutive seven-day periods in at most 31 calendar days. Returns separate highest-total and first-to-last change rankings with every period value. Percentage change is omitted for a zero first-period baseline. Defaults to regular Local/Rapid/Tempo routes and the 28 days ending yesterday; special-service groups remain separate."""
    return data.rank_route_trends(
        metric,
        start_date,
        end_date,
        period,
        order,
        limit,
        route_group,
    )


@mcp.tool
def get_route_time_profile(
    route_id: str,
    metric: Literal["median_delay", "p95_delay", "observed_arrivals"] = "median_delay",
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """Return a route's bounded hourly delay or observed-arrival profile. Dates default to four weeks ending yesterday."""
    return data.get_route_time_profile(route_id, metric, start_date, end_date)


@mcp.tool
def list_scheduled_arrivals(
    route_id: str,
    service_date: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    direction_id: int | None = None,
    stop_id: str | None = None,
    trip_id: str | None = None,
    vehicle_id: str | None = None,
) -> dict:
    """List at most 250 scheduled stop arrivals in a window of at most four hours and mark observed or missing arrivals. The date defaults to yesterday."""
    return data.list_scheduled_arrivals(
        route_id,
        service_date,
        start_time,
        end_time,
        direction_id,
        stop_id,
        trip_id,
        vehicle_id,
    )


@mcp.tool
def get_metric_definition(metric: str) -> dict:
    """Return the public definition and display unit for an approved performance metric."""
    definitions = {
        "on_time_service_delivered": "Eligible scheduled stop arrivals reached between one minute early and seven minutes late.",
        "trip_delivery": "The share of scheduled trips that were observed running; the complement is Dropped / not observed.",
        "on_time_within_3_minutes": "Observed stop arrivals between scheduled time and three minutes late.",
        "median_delay": "The median observed stop-arrival delay in minutes.",
        "p95_delay": "The 95th percentile observed stop-arrival delay in minutes.",
        "average_speed": "Average observed inter-stop speed in miles per hour.",
        "dropped_trips": "Scheduled trips with no observed running trip.",
        "bunched_headways": "Observed headways that meet the current bunching methodology threshold.",
        "long_gaps": "Observed headways that meet the current long-gap methodology threshold.",
    }
    if metric not in definitions:
        raise ValueError(f"metric must be one of: {', '.join(definitions)}")
    return {
        "answer_context": definitions[metric],
        "timeframe": None,
        "maps": [],
        "charts": [],
        "tables": [],
        "sources": [{"kind": "methodology_registry"}],
        "warnings": [],
        "definition": definitions[metric],
        "label": METRICS[metric]["label"],
        "unit": METRICS[metric]["unit"],
    }


@mcp.tool(
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
def describe_data(
    table: Literal[
        "trip_observations",
        "trip_probes",
        "ridership_observations",
        "api_request_observations",
        "api_request_hourly",
    ]
    | None = None,
) -> dict:
    """Describe the allowlisted tables, columns, types, semantics, and structured-query limits. Use this only when the purpose-built analysis tools do not cover the question or when the user asks about available data. It never accepts SQL."""
    return data.describe_data(table)


@mcp.tool(
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
def query_data(
    table: Literal[
        "trip_observations",
        "trip_probes",
        "ridership_observations",
        "api_request_observations",
        "api_request_hourly",
    ],
    columns: list[str] | None = None,
    aggregations: list[QueryAggregation] | None = None,
    filters: list[QueryFilter] | None = None,
    group_by: list[str] | None = None,
    order_by: list[QueryOrder] | None = None,
    limit: int = 50,
    start_date: str | None = None,
    end_date: str | None = None,
    visualization: QueryVisualization | None = None,
) -> dict:
    """Run a bounded exploratory query when no purpose-built tool fully answers the user's AC Transit question. Prefer the specialized tools because they encode public metric methodology and are usually cheaper. This fallback accepts only an allowlisted table, exact schema columns, typed filters, approved aggregates, grouping, sorting, and an optional structured visualization; it never accepts SQL, expressions, joins, or object names. Every query is limited to 31 completed service dates, 200 result rows, the server byte cap, and read-only IAM; dates default to four weeks ending yesterday. Call describe_data first if column semantics are uncertain."""
    return data.query_data(
        table=table,
        columns=columns,
        aggregations=model_values(aggregations or []),
        filters=model_values(filters or []),
        group_by=group_by,
        order_by=model_values(order_by or []),
        limit=limit,
        start_date=start_date,
        end_date=end_date,
        visualization=model_value(visualization) if visualization else None,
    )


@mcp.custom_route("/health", methods=["GET"])
async def health(_request) -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "actransit-data-mcp", "profile_metrics": list(PROFILE_METRICS)})


if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
