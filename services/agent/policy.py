from __future__ import annotations

import json
import re
from datetime import date, timedelta
from typing import Any


ROUTE_SCOPED_TOOLS = {
    "get_route_delay_map",
    "get_route_performance",
    "get_route_segment_performance_map",
    "get_route_service_profile",
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
    if tool_name == "rank_routes" and not prepared.get("route_group"):
        prepared["route_group"] = "regular"
    if route_id and tool_name in {
        "get_route_delay_map",
        "get_route_performance",
        "get_route_segment_performance_map",
        "get_route_service_profile",
        "get_route_time_profile",
        "list_scheduled_arrivals",
    }:
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
        elif tool_name in {
            "get_route_delay_map",
            "get_route_performance",
            "get_route_segment_performance_map",
            "get_route_service_profile",
            "get_route_time_profile",
        }:
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


def eval_prompt_features(prompt: str) -> list[str]:
    lowered = prompt.lower()
    features = []
    patterns = {
        "ranking": r"\b(worst|best|rank|ranking|worse)\b",
        "comparison": r"\b(compare|comparison|versus|vs\.?|worse than|better than)\b",
        "hourly": r"\b(hour|hours|hourly|time of day)\b",
        "dropped_trip_timing": (
            r"\b(not run|did not run|dropped|unobserved|missing trips?)\b.*"
            r"\b(hour|hours|hourly|each day|by day)\b|"
            r"\b(hour|hours|hourly|each day|by day)\b.*"
            r"\b(not run|did not run|dropped|unobserved|missing trips?)\b"
        ),
        "popularity_language": r"\b(popular|common|major|busy)\b",
        "school_service": r"\b(6\d{2}|600.series|school)\b",
        "all_nighter_service": r"\b(8\d{2}|800.series|all.nighter|overnight)\b",
        "spatial": r"\b(map|where|location|segment|section|corridor|intersection|turn|stop.to.stop)\b",
    }
    for feature, pattern in patterns.items():
        if re.search(pattern, lowered):
            features.append(feature)
    return features


def enforce_route_group(tool_name: str, arguments: dict[str, Any], prompt: str) -> dict[str, Any]:
    if tool_name != "rank_routes":
        return arguments

    lowered = prompt.lower()
    explicit_groups: set[str] = set()
    if re.search(r"\b(6\d{2}|600.series|school)\b", lowered):
        explicit_groups.add("school")
    if re.search(r"\b(8\d{2}|800.series|all.nighter|overnight)\b", lowered):
        explicit_groups.add("all_nighter")
    if re.search(r"\b(transbay|lettered route)\b", lowered):
        explicit_groups.add("transbay")
    if re.search(r"\b(local|regular|rapid|tempo|popular|common|major)\b", lowered):
        explicit_groups.add("regular")
    allowed = explicit_groups or {"regular"}

    updated = dict(arguments)
    requested = str(updated.get("route_group") or "regular")
    if requested not in allowed:
        if len(allowed) == 1:
            updated["route_group"] = next(iter(allowed))
        else:
            raise ValueError(
                f"The requested {requested} ranking was not one of the service groups named by the user"
            )
    return updated


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
    request_id: str | None = None,
) -> list[dict[str, Any]]:
    if len(context) > max_follow_ups:
        raise ValueError("The follow-up limit for this answer has been reached")
    turn: dict[str, Any] = {
        "question": prompt,
        "answer": str(result.get("answer") or "")[:CONTEXT_ANSWER_CHARS],
    }
    if request_id:
        turn["request_id"] = request_id
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
    chart_candidates: list[dict[str, Any]] = []
    maps: list[dict[str, Any]] = []
    map_candidates: list[dict[str, Any]] = []
    tables: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    warnings: list[str] = []
    seen_sources: set[str] = set()
    total_chart_points = 0

    for result in results:
        timeframe = result.get("timeframe")
        if timeframe:
            timeframes.append(timeframe)

        chart_candidates.extend(result.get("charts") or [])
        map_candidates.extend(result.get("maps") or [])

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

    map_candidates.sort(key=lambda item: int(item.get("priority") or 0), reverse=True)
    total_map_features = 0
    total_map_coordinates = 0
    for item in map_candidates:
        if len(maps) >= 3:
            break
        geojson = item.get("geojson") or {}
        candidates = list(geojson.get("features") or [])
        features = []
        for feature in candidates:
            coordinate_count = count_geojson_coordinates((feature.get("geometry") or {}).get("coordinates"))
            if total_map_features >= 400 or total_map_coordinates + coordinate_count > 3500:
                continue
            features.append(feature)
            total_map_features += 1
            total_map_coordinates += coordinate_count
        if features:
            normalized = {key: value for key, value in item.items() if key != "priority"}
            normalized["geojson"] = {
                "type": "FeatureCollection",
                "features": features,
            }
            maps.append(normalized)

    chart_candidates.sort(key=lambda chart: int(chart.get("priority") or 0), reverse=True)
    for chart in chart_candidates:
        if len(charts) >= 3:
            break
        remaining = 500 - total_chart_points
        if remaining <= 0:
            break
        normalized = {key: value for key, value in chart.items() if key != "priority"}
        if chart.get("type") == "heatmap":
            cells = list(chart.get("cells") or [])[: min(168, remaining)]
            normalized["cells"] = cells
            normalized["x_labels"] = list(chart.get("x_labels") or [])[:24]
            normalized["y_labels"] = list(chart.get("y_labels") or [])[:7]
            normalized["labels"] = []
            normalized["series"] = []
            total_chart_points += len(cells)
        else:
            labels = list(chart.get("labels") or [])[: min(200, remaining)]
            normalized["labels"] = labels
            normalized["series"] = []
            for series in (chart.get("series") or [])[:5]:
                normalized["series"].append(
                    {**series, "values": list(series.get("values") or [])[: len(labels)]}
                )
            total_chart_points += len(labels)
        charts.append(normalized)

    return {
        "timeframes": consolidate_timeframes(timeframes),
        "maps": maps,
        "charts": charts,
        "tables": tables,
        "sources": sources,
        "warnings": warnings,
    }


def count_geojson_coordinates(value: Any) -> int:
    if not isinstance(value, list):
        return 0
    if len(value) >= 2 and all(isinstance(item, (int, float)) for item in value[:2]):
        return 1
    return sum(count_geojson_coordinates(item) for item in value)


def compact_tool_result(result: dict[str, Any]) -> dict[str, Any]:
    compact = dict(result)
    maps = compact.pop("maps", [])
    if maps:
        compact["map_summaries"] = [
            {
                "title": item.get("title"),
                "description": item.get("description"),
                "feature_count": len((item.get("geojson") or {}).get("features") or []),
                "value_label": (item.get("style") or {}).get("value_label"),
            }
            for item in maps[:3]
        ]
    return compact
