from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CASES_PATH = Path(__file__).with_name("conversation_cases.json")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run bounded live multi-turn analysis evals.")
    parser.add_argument("--api-url", required=True)
    args = parser.parse_args()
    cases = json.loads(CASES_PATH.read_text())
    failed = 0

    for case in cases:
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
            payload = request_turn(
                args.api_url,
                session_id,
                parent_request_id,
                case.get("scope") or {},
                turn["prompt"],
            )
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

    print(json.dumps({"cases": len(cases), "failed": failed, "passed": len(cases) - failed}))
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

    chart_types = {str(chart.get("type")) for chart in payload.get("charts") or []}
    required_types = set(turn.get("required_chart_types") or [])
    if not required_types.issubset(chart_types):
        errors.append(f"missing chart types: {sorted(required_types - chart_types)}")

    titles = [str(chart.get("title") or "").lower() for chart in payload.get("charts") or []]
    for term in turn.get("required_chart_title_terms") or []:
        if not any(term.lower() in title for title in titles):
            errors.append(f"missing chart title term: {term}")

    answer = str(payload.get("answer") or "").lower()
    for term in turn.get("forbidden_answer_terms") or []:
        if term.lower() in answer:
            errors.append(f"forbidden answer term returned: {term}")
    return errors


if __name__ == "__main__":
    sys.exit(main())
