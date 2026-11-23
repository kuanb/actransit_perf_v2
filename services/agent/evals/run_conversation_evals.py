from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


CASES_PATH = Path(__file__).with_name("conversation_cases.json")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run bounded live multi-turn analysis evals.")
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--case", action="append", dest="case_ids", help="Run only this case ID; repeat for more cases")
    args = parser.parse_args()
    cases = json.loads(CASES_PATH.read_text())
    selected_cases = [case for case in cases if not args.case_ids or case["id"] in args.case_ids]
    if args.case_ids and len(selected_cases) != len(set(args.case_ids)):
        parser.error("Every --case value must name a case in the suite")
    failed = 0

    for case in selected_cases:
        session_id = f"conversation-eval-{uuid.uuid4().hex}"
        parent_request_id = None
        next_allowed_at: datetime | None = None
        case_errors = []
        turn_results = []
        for turn_index, turn in enumerate(case["turns"], start=1):
            if next_allowed_at:
                remaining = (next_allowed_at - datetime.now(timezone.utc)).total_seconds()
                if remaining > 0:
                    time.sleep(remaining + 0.5)
            try:
                payload = request_turn(
                    args.api_url,
                    session_id,
                    parent_request_id,
                    case.get("scope") or {},
                    turn["prompt"],
                )
            except Exception as exc:
                error = f"request failed: {type(exc).__name__}: {exc}"
                case_errors.append(f"turn {turn_index}: {error}")
                turn_results.append({"turn": turn_index, "errors": [error]})
                break
            errors = validate_turn(turn, payload)
            case_errors.extend(f"turn {turn_index}: {error}" for error in errors)
            turn_results.append(
                {
                    "turn": turn_index,
                    "request_id": payload.get("request_id"),
                    "map_count": len(payload.get("maps") or []),
                    "chart_types": [chart.get("type") for chart in payload.get("charts") or []],
                    "errors": errors,
                }
            )
            parent_request_id = payload.get("request_id")
            next_value = payload.get("next_allowed_at")
            next_allowed_at = datetime.fromisoformat(next_value) if next_value else None

        failed += bool(case_errors)
        print(
            json.dumps(
                {
                    "case": case["id"],
                    "passed": not case_errors,
                    "errors": case_errors,
                    "turns": turn_results,
                }
            )
        )

    print(
        json.dumps(
            {
                "cases": len(selected_cases),
                "failed": failed,
                "passed": len(selected_cases) - failed,
            }
        )
    )
    return 1 if failed else 0


def request_turn(
    api_url: str,
    session_id: str,
    parent_request_id: str | None,
    scope: dict[str, Any],
    prompt: str,
) -> dict[str, Any]:
    request_id = f"conversation-eval-{uuid.uuid4().hex}"
    body = json.dumps(
        {
            "prompt": prompt,
            "session_id": session_id,
            "request_id": request_id,
            "parent_request_id": parent_request_id,
            "scope": scope,
        }
    ).encode()
    request = urllib.request.Request(
        f"{api_url.rstrip('/')}/v1/query",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"Eval request failed with HTTP {exc.code}: {detail}") from exc


def validate_turn(turn: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    errors = []
    maps = payload.get("maps") or []
    minimum_map_count = int(turn.get("minimum_map_count") or 0)
    if len(maps) < minimum_map_count:
        errors.append(f"expected at least {minimum_map_count} maps, received {len(maps)}")

    map_targets = {str((item.get("style") or {}).get("target")) for item in maps}
    required_map_targets = set(turn.get("required_map_targets") or [])
    if not required_map_targets.issubset(map_targets):
        errors.append(f"missing map targets: {sorted(required_map_targets - map_targets)}")

    map_titles = [str(item.get("title") or "").lower() for item in maps]
    for term in turn.get("required_map_title_terms") or []:
        if not any(term.lower() in title for title in map_titles):
            errors.append(f"missing map title term: {term}")

    map_metrics: dict[str, list[float]] = {}
    for item in maps:
        for feature in (item.get("geojson") or {}).get("features") or []:
            properties = feature.get("properties") or {}
            kind = str(properties.get("highlight_kind") or "")
            value = properties.get("metric_value")
            if kind and isinstance(value, (int, float)):
                map_metrics.setdefault(kind, []).append(float(value))
    for kind, sign in (turn.get("required_map_metric_signs") or {}).items():
        values = map_metrics.get(kind) or []
        matches = values and (
            (sign == "positive" and any(value > 0 for value in values))
            or (sign == "negative" and any(value < 0 for value in values))
        )
        if not matches:
            errors.append(f"missing {sign} map metric for highlight kind: {kind}")

    charts = payload.get("charts") or []
    minimum_chart_count = int(turn.get("minimum_chart_count") or 0)
    if len(charts) < minimum_chart_count:
        errors.append(f"expected at least {minimum_chart_count} charts, received {len(charts)}")

    chart_types = {str(chart.get("type")) for chart in charts}
    required_types = set(turn.get("required_chart_types") or [])
    if not required_types.issubset(chart_types):
        errors.append(f"missing chart types: {sorted(required_types - chart_types)}")

    titles = [str(chart.get("title") or "").lower() for chart in charts]
    for term in turn.get("required_chart_title_terms") or []:
        if not any(term.lower() in title for title in titles):
            errors.append(f"missing chart title term: {term}")

    x_labels = [str(chart.get("x_label") or "").lower() for chart in charts]
    for term in turn.get("required_chart_x_label_terms") or []:
        if not any(term.lower() in label for label in x_labels):
            errors.append(f"missing chart x-axis label term: {term}")
    numeric_x_charts = sum(
        bool(chart.get("x_values"))
        and len(chart.get("x_values") or []) == len(chart.get("labels") or [])
        for chart in charts
    )
    required_numeric_x = int(turn.get("minimum_numeric_x_chart_count") or 0)
    if numeric_x_charts < required_numeric_x:
        errors.append(
            f"expected at least {required_numeric_x} charts with numeric x positions, "
            f"received {numeric_x_charts}"
        )

    timeframes = payload.get("timeframes") or []
    timeframe = timeframes[0] if timeframes else {}
    required_days = int(turn.get("required_timeframe_days") or 0)
    if required_days:
        try:
            actual_days = (
                date.fromisoformat(str(timeframe["end_date"]))
                - date.fromisoformat(str(timeframe["start_date"]))
            ).days + 1
        except (KeyError, ValueError):
            actual_days = 0
        if actual_days != required_days:
            errors.append(f"expected a {required_days}-day timeframe, received {actual_days} days")
    required_basis = turn.get("required_timeframe_basis")
    if required_basis and timeframe.get("basis") != required_basis:
        errors.append(
            f"expected timeframe basis {required_basis}, received {timeframe.get('basis')}"
        )
    if turn.get("require_assumed_timeframe") and timeframe.get("assumed") is not True:
        errors.append("expected the response to disclose an assumed timeframe")

    answer = str(payload.get("answer") or "").lower()
    for term in turn.get("required_answer_terms") or []:
        if term.lower() not in answer:
            errors.append(f"missing answer term: {term}")
    any_terms = [term.lower() for term in turn.get("required_answer_any_terms") or []]
    if any_terms and not any(term in answer for term in any_terms):
        errors.append(f"missing any answer term: {any_terms}")
    for term in turn.get("forbidden_answer_terms") or []:
        if term.lower() in answer:
            errors.append(f"forbidden answer term returned: {term}")
    return errors


if __name__ == "__main__":
    sys.exit(main())
