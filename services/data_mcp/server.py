from __future__ import annotations

import os
from typing import Literal

from fastmcp import FastMCP
from starlette.responses import JSONResponse

from core import METRICS
from gcp_data import PROFILE_METRICS, TransitData


mcp = FastMCP(
    "AC Transit performance data",
    instructions=(
        "Read-only, bounded tools for published AC Transit performance reports and observations. "
        "When dates are omitted, tools use the seven-day period ending yesterday in America/Los_Angeles."
    ),
)
data = TransitData()


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
    """Compare two to five routes using one approved metric and at most 31 calendar days. Dates default to seven days ending yesterday."""
    return data.compare_routes(route_ids, metric, start_date, end_date)


@mcp.tool
def get_route_performance(
    route_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """Summarize one route across rider-window, trip-delivery, delay, speed, bunching, and gap metrics. Dates default to seven days ending yesterday."""
    return data.get_route_performance(route_id, start_date, end_date)


@mcp.tool
def get_system_performance(
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """Summarize systemwide service delivery, delay, speed, bunching, and gap metrics. Dates default to seven days ending yesterday."""
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
) -> dict:
    """Rank at most ten routes using an approved metric. Dates default to seven days ending yesterday."""
    return data.rank_routes(metric, start_date, end_date, order, limit)


@mcp.tool
def get_route_time_profile(
    route_id: str,
    metric: Literal["median_delay", "p95_delay", "observed_arrivals"] = "median_delay",
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """Return a route's bounded hourly delay or observed-arrival profile. Dates default to seven days ending yesterday."""
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
        "charts": [],
        "tables": [],
        "sources": [{"kind": "methodology_registry"}],
        "warnings": [],
        "definition": definitions[metric],
        "label": METRICS[metric]["label"],
        "unit": METRICS[metric]["unit"],
    }


@mcp.custom_route("/health", methods=["GET"])
async def health(_request) -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "actransit-data-mcp", "profile_metrics": list(PROFILE_METRICS)})


if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
