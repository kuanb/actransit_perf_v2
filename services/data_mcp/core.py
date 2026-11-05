from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any


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


def resolve_daily_dates(
    published_dates: list[str],
    start_date: str | None,
    end_date: str | None,
    default_service_days: int,
    default_label: str,
) -> ResolvedDates:
    if not published_dates:
        raise ValueError("No published daily reports are available")

    ordered = sorted(set(published_dates))
    assumed = start_date is None and end_date is None
    warnings: list[str] = []

    if assumed:
        chosen = ordered[-default_service_days:]
        disclosure = (
            f"No timeframe was provided, so this analysis uses {default_label}: "
            f"{chosen[0]} through {chosen[-1]}."
        )
        basis = default_label.replace(" ", "_")
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

    return ResolvedDates(
        dates=chosen,
        timeframe={
            "start_date": chosen[0],
            "end_date": chosen[-1],
            "assumed": assumed,
            "basis": basis,
            "disclosure": disclosure,
        },
        warnings=warnings,
    )


def previous_sunday_through_saturday(latest_date: str) -> tuple[str, str]:
    latest = date.fromisoformat(latest_date)
    saturday = latest - timedelta(days=(latest.weekday() - 5) % 7)
    sunday = saturday - timedelta(days=6)
    return sunday.isoformat(), saturday.isoformat()


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
