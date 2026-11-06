from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any


ROUTE_SCOPED_TOOLS = {
    "get_route_performance",
    "get_route_time_profile",
    "list_scheduled_arrivals",
    "get_metric_definition",
}
GENERAL_TOOLS = ROUTE_SCOPED_TOOLS | {"compare_routes", "get_system_performance", "rank_routes"}
CONTEXT_ANSWER_CHARS = 2000


def allowed_tool_names(route_id: str | None) -> set[str]:
    return ROUTE_SCOPED_TOOLS if route_id else GENERAL_TOOLS


def prepare_tool_call(
    tool_name: str,
    arguments: dict[str, Any],
    route_id: str | None,
    week_end: str | None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if tool_name not in allowed_tool_names(route_id):
        raise ValueError(f"Tool {tool_name} is not available in this analysis scope")

    prepared = dict(arguments)
    if route_id and tool_name in {"get_route_performance", "get_route_time_profile", "list_scheduled_arrivals"}:
        prepared["route_id"] = route_id

    context_timeframe = None
    if week_end and not any(prepared.get(key) for key in ("start_date", "end_date", "service_date")):
        end = date.fromisoformat(week_end)
        start = end - timedelta(days=6)
        if tool_name == "list_scheduled_arrivals":
            prepared["service_date"] = week_end
            context_timeframe = {
                "start_date": week_end,
                "end_date": week_end,
                "assumed": True,
                "basis": "route_page_context",
                "disclosure": (
                    "No date was provided, so this analysis uses the service date shown by the route page: "
                    f"{week_end}."
                ),
            }
        elif tool_name in {"get_route_performance", "get_route_time_profile"}:
            prepared["start_date"] = start.isoformat()
            prepared["end_date"] = end.isoformat()
            context_timeframe = {
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "assumed": True,
                "basis": "route_page_context",
                "disclosure": (
                    "No timeframe was provided, so this analysis uses the week shown by the route page: "
                    f"{start.isoformat()} through {end.isoformat()}."
                ),
            }
    return prepared, context_timeframe


def apply_context_timeframe(result: dict[str, Any], context_timeframe: dict[str, Any] | None) -> dict[str, Any]:
    if not context_timeframe:
        return result
    updated = dict(result)
    existing = dict(updated.get("timeframe") or {})
    existing.update(context_timeframe)
    if "start_time" in (updated.get("timeframe") or {}):
        existing["start_time"] = updated["timeframe"]["start_time"]
        existing["end_time"] = updated["timeframe"]["end_time"]
        existing["timezone"] = updated["timeframe"].get("timezone")
        existing["disclosure"] += (
            f" No clock time was provided, so the tool used {existing['start_time']}–{existing['end_time']} Pacific time."
        )
    updated["timeframe"] = existing
    return updated


def extend_conversation(
    context: list[dict[str, Any]],
    prompt: str,
    result: dict[str, Any],
    max_follow_ups: int,
) -> list[dict[str, Any]]:
    if len(context) > max_follow_ups:
        raise ValueError("The follow-up limit for this answer has been reached")
    turn: dict[str, Any] = {
        "question": prompt,
        "answer": str(result.get("answer") or "")[:CONTEXT_ANSWER_CHARS],
    }
    timeframes = list(result.get("timeframes") or [])
    if timeframes:
        turn["timeframe"] = timeframes[0]
    return [*context, turn]


def follow_ups_remaining(context: list[dict[str, Any]], max_follow_ups: int) -> int:
    return max(0, max_follow_ups - max(0, len(context) - 1))


def consolidate_timeframes(timeframes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: list[dict[str, Any]] = []
    identities: set[str] = set()
    for timeframe in timeframes:
        identity = json.dumps(
            {
                key: timeframe.get(key)
                for key in ("start_date", "end_date", "start_time", "end_time", "timezone")
            },
            sort_keys=True,
            default=str,
        )
        if identity not in identities:
            identities.add(identity)
            unique.append(timeframe)

    if len(unique) <= 1:
        return unique

    disclosures = [
        str(timeframe.get("disclosure"))
        for timeframe in unique
        if timeframe.get("disclosure")
    ]
    return [
        {
            "start_date": min(str(timeframe.get("start_date")) for timeframe in unique),
            "end_date": max(str(timeframe.get("end_date")) for timeframe in unique),
            "assumed": any(bool(timeframe.get("assumed")) for timeframe in unique),
            "basis": "combined_tool_timeframes",
            "disclosure": "This answer combines multiple analyzed periods: " + " ".join(disclosures),
        }
    ]


def merge_tool_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    timeframes: list[dict[str, Any]] = []
    charts: list[dict[str, Any]] = []
    tables: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    warnings: list[str] = []
    seen_sources: set[str] = set()
    total_chart_points = 0

    for result in results:
        timeframe = result.get("timeframe")
        if timeframe:
            timeframes.append(timeframe)

        for chart in result.get("charts") or []:
            if len(charts) >= 3:
                break
            labels = list(chart.get("labels") or [])[:200]
            remaining = 500 - total_chart_points
            if remaining <= 0:
                break
            labels = labels[:remaining]
            normalized = {**chart, "labels": labels, "series": []}
            for series in (chart.get("series") or [])[:5]:
                normalized["series"].append({**series, "values": list(series.get("values") or [])[: len(labels)]})
            total_chart_points += len(labels)
            charts.append(normalized)

        for table in result.get("tables") or []:
            if len(tables) >= 2:
                break
            tables.append({**table, "rows": list(table.get("rows") or [])[:250]})

        for source in result.get("sources") or []:
            key = json.dumps(source, sort_keys=True, default=str)
            if key not in seen_sources and len(sources) < 8:
                seen_sources.add(key)
                sources.append(source)

        for warning in result.get("warnings") or []:
            if warning not in warnings and len(warnings) < 8:
                warnings.append(str(warning))

    return {
        "timeframes": consolidate_timeframes(timeframes),
        "charts": charts,
        "tables": tables,
        "sources": sources,
        "warnings": warnings,
    }
