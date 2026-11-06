from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import re
from typing import Any
from zoneinfo import ZoneInfo


PACIFIC = ZoneInfo("America/Los_Angeles")


ROUTE_GROUPS = {
    "regular": {
        "label": "Local / Rapid / Tempo",
        "definition": (
            "Regular East Bay peer group used for performance rankings. It excludes 600-series "
            "Supplementary School Service, 800-series All Nighter service, and lettered Transbay routes."
        ),
        "source_url": "https://www.actransit.org/maps-schedules",
    },
    "school": {
        "label": "600-series Supplementary School Service",
        "definition": (
            "AC Transit's 600-series service to schools operates around student demand and the academic calendar."
        ),
        "source_url": (
            "https://www.actransit.org/press-release/"
            "ac-transit-rolls-out-back-to-school-service-starting-august-9"
        ),
    },
    "all_nighter": {
        "label": "800-series All Nighter",
        "definition": (
            "AC Transit's 800-series routes are part of its late-night and early-morning All Nighter network."
        ),
        "source_url": "https://www.actransit.org/index.php/all-nighter",
    },
    "transbay": {
        "label": "Transbay",
        "definition": "Lettered Transbay routes are compared separately from East Bay regular service.",
        "source_url": "https://www.actransit.org/maps-schedules",
    },
}


METRICS = {
    "on_time_service_delivered": {
        "label": "On Time Service Delivered",
        "unit": "percent",
        "kind": "ratio",
        "numerator": "stop_sd_delivered_n",
        "denominator": "stop_sd_n",
        "higher_is_worse": False,
    },
    "trip_delivery": {
        "label": "Trip delivery",
        "unit": "percent",
        "kind": "ratio",
        "numerator": "ran_trips",
        "denominator": "scheduled_trips",
        "higher_is_worse": False,
    },
    "on_time_within_3_minutes": {
        "label": "On time within 3 minutes",
        "unit": "percent",
        "kind": "weighted",
        "field": "on_time_pct",
        "weight": "observations",
        "higher_is_worse": False,
    },
    "median_delay": {
        "label": "Median delay",
        "unit": "minutes",
        "kind": "daily_mean",
        "field": "p50_delay_minutes",
        "higher_is_worse": True,
    },
    "p95_delay": {
        "label": "95th percentile delay",
        "unit": "minutes",
        "kind": "daily_mean",
        "field": "p95_delay_minutes",
        "higher_is_worse": True,
    },
    "average_speed": {
        "label": "Average speed",
        "unit": "mph",
        "kind": "weighted",
        "field": "avg_speed_mph",
        "weight": "observations",
        "higher_is_worse": False,
    },
    "dropped_trips": {
        "label": "Dropped / not observed trips",
        "unit": "trips",
        "kind": "derived_sum",
        "higher_is_worse": True,
    },
    "bunched_headways": {
        "label": "Bunched headways",
        "unit": "percent",
        "kind": "nested_ratio",
        "numerator": "bunched_headway_n",
        "denominator": "headway_n",
        "higher_is_worse": True,
    },
    "long_gaps": {
        "label": "Long gaps",
        "unit": "percent",
        "kind": "nested_ratio",
        "numerator": "long_gap_n",
        "denominator": "headway_n",
        "higher_is_worse": True,
    },
}


@dataclass(frozen=True)
class ResolvedDates:
    dates: list[str]
    timeframe: dict[str, Any]
    warnings: list[str]


def route_group_for_id(route_id: str) -> str:
    normalized = route_id.strip().upper()
    if re.fullmatch(r"6\d{2}", normalized):
        return "school"
    if re.fullmatch(r"8\d{2}", normalized):
        return "all_nighter"
    if normalized[:1].isalpha():
        return "transbay"
    return "regular"


def comparable_route_group(route_ids: list[str]) -> str:
    groups = {route_group_for_id(route_id) for route_id in route_ids}
    if len(groups) != 1:
        labels = ", ".join(sorted(ROUTE_GROUPS[group]["label"] for group in groups))
        raise ValueError(
            f"Routes from different service groups cannot be compared directly ({labels}). "
            "Analyze each service group separately."
        )
    return groups.pop()


def route_ids_for_group(route_ids: list[str], route_group: str) -> list[str]:
    if route_group not in ROUTE_GROUPS:
        raise ValueError(f"route_group must be one of: {', '.join(ROUTE_GROUPS)}")
    return sorted(
        {route_id for route_id in route_ids if route_group_for_id(route_id) == route_group}
    )


def route_group_source(route_group: str) -> dict[str, Any]:
    definition = ROUTE_GROUPS[route_group]
    return {
        "kind": "route_classification",
        "route_group": route_group,
        "label": definition["label"],
        "definition": definition["definition"],
        "url": definition["source_url"],
    }


def resolve_daily_dates(
    published_dates: list[str],
    start_date: str | None,
    end_date: str | None,
    today: date | None = None,
) -> ResolvedDates:
    if not published_dates:
        raise ValueError("No published daily reports are available")

    ordered = sorted(set(published_dates))
    assumed = start_date is None and end_date is None
    warnings: list[str] = []

    if assumed:
        window_start, window_end = default_analysis_window(today)
        chosen = [value for value in ordered if window_start <= value <= window_end]
        if not chosen:
            raise ValueError("No published reports exist in the default seven-day period")
        if len(chosen) < 7:
            warnings.append(
                "The default seven-day period includes dates without a published report; only published service dates were analyzed."
            )
        disclosure = (
            "No timeframe was provided, so this analysis uses the seven-day period ending yesterday: "
            f"{window_start} through {window_end}."
        )
        basis = "seven_day_period_ending_yesterday"
        timeframe_start = window_start
        timeframe_end = window_end
    else:
        if start_date is None:
            start_date = end_date
        if end_date is None:
            end_date = start_date
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        if end < start:
            raise ValueError("end_date must be on or after start_date")
        if (end - start).days > 30:
            raise ValueError("The maximum analysis window is 31 calendar days")
        chosen = [value for value in ordered if start_date <= value <= end_date]
        if not chosen:
            raise ValueError("No published reports exist in the requested date range")
        requested_days = (end - start).days + 1
        if len(chosen) < requested_days:
            warnings.append(
                "The requested range includes dates without a published report; only published service dates were analyzed."
            )
        disclosure = f"Analyzed published service dates from {chosen[0]} through {chosen[-1]}."
        basis = "user_supplied_or_interpreted_dates"
        timeframe_start = chosen[0]
        timeframe_end = chosen[-1]

    return ResolvedDates(
        dates=chosen,
        timeframe={
            "start_date": timeframe_start,
            "end_date": timeframe_end,
            "assumed": assumed,
            "basis": basis,
            "disclosure": disclosure,
        },
        warnings=warnings,
    )


def default_analysis_window(today: date | None = None) -> tuple[str, str]:
    current = today or datetime.now(PACIFIC).date()
    end = current - timedelta(days=1)
    start = end - timedelta(days=6)
    return start.isoformat(), end.isoformat()


def daily_metric_value(route: dict[str, Any], metric: str) -> float | None:
    spec = METRICS[metric]
    kind = spec["kind"]
    if kind == "ratio":
        denominator = number(route.get(spec["denominator"]))
        numerator = number(route.get(spec["numerator"]))
        if not denominator:
            return None
        return round(numerator / denominator * 100, 1)
    if kind == "nested_ratio":
        bunching = route.get("bunching") or {}
        denominator = number(bunching.get(spec["denominator"]))
        numerator = number(bunching.get(spec["numerator"]))
        if not denominator:
            return None
        return round(numerator / denominator * 100, 1)
    if kind == "derived_sum":
        scheduled = number(route.get("scheduled_trips"))
        ran = number(route.get("ran_trips"))
        return max(0.0, scheduled - ran)
    value = route.get(spec["field"])
    if value is None:
        return None
    return round(float(value), 1)


def aggregate_metric(routes: list[dict[str, Any]], metric: str) -> tuple[float | None, int]:
    spec = METRICS[metric]
    kind = spec["kind"]
    usable = [route for route in routes if daily_metric_value(route, metric) is not None]
    if not usable:
        return None, 0

    if kind == "ratio":
        numerator = sum(number(route.get(spec["numerator"])) for route in usable)
        denominator = sum(number(route.get(spec["denominator"])) for route in usable)
        return (round(numerator / denominator * 100, 1) if denominator else None), len(usable)
    if kind == "nested_ratio":
        numerator = sum(number((route.get("bunching") or {}).get(spec["numerator"])) for route in usable)
        denominator = sum(number((route.get("bunching") or {}).get(spec["denominator"])) for route in usable)
        return (round(numerator / denominator * 100, 1) if denominator else None), len(usable)
    if kind == "weighted":
        weighted_sum = 0.0
        total_weight = 0.0
        for route in usable:
            weight = number(route.get(spec["weight"]))
            value = daily_metric_value(route, metric)
            if value is None or weight <= 0:
                continue
            weighted_sum += value * weight
            total_weight += weight
        return (round(weighted_sum / total_weight, 1) if total_weight else None), len(usable)
    if kind == "derived_sum":
        return round(sum(daily_metric_value(route, metric) or 0 for route in usable), 1), len(usable)

    values = [daily_metric_value(route, metric) for route in usable]
    return round(sum(value for value in values if value is not None) / len(values), 1), len(usable)


def format_metric(value: float | None, metric: str) -> str:
    if value is None:
        return "No data"
    unit = METRICS[metric]["unit"]
    if unit == "percent":
        return f"{value:.1f}%"
    if unit == "minutes":
        return f"{value:.1f} min"
    if unit == "mph":
        return f"{value:.1f} mph"
    return f"{int(round(value)):,} trips"


def system_metric_row(report: dict[str, Any]) -> dict[str, Any]:
    system = dict(report.get("system") or {})
    compliance = report.get("schedule_compliance") or {}
    system.update(
        {
            "stop_sd_delivered_n": compliance.get("stop_sd_delivered_n"),
            "stop_sd_n": compliance.get("stop_sd_n"),
            "ran_trips": compliance.get("ran_trips"),
            "scheduled_trips": compliance.get("scheduled_trips"),
            "observations": system.get("total_observations"),
        }
    )
    return system


def number(value: Any) -> float:
    if value is None:
        return 0.0
    return float(value)
