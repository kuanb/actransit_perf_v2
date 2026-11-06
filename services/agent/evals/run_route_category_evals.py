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


CASES_PATH = Path(__file__).with_name("route_category_cases.json")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the bounded live route-category model eval suite."
    )
    parser.add_argument("--api-url", required=True)
    args = parser.parse_args()

    cases = json.loads(CASES_PATH.read_text())
    session_id = f"route-category-eval-{uuid.uuid4().hex}"
    next_allowed_at: datetime | None = None
    failed = 0

    for case in cases:
        if next_allowed_at:
            remaining = (next_allowed_at - datetime.now(timezone.utc)).total_seconds()
            if remaining > 0:
                time.sleep(remaining + 0.5)

        payload = request_case(args.api_url, session_id, case)
        next_value = payload.get("next_allowed_at")
        next_allowed_at = datetime.fromisoformat(next_value) if next_value else None
        errors = validate_case(case, payload)
        failed += bool(errors)
        print(
            json.dumps(
                {
                    "case": case["id"],
                    "passed": not errors,
                    "errors": errors,
                    "route_groups": sorted(response_route_groups(payload)),
                    "request_id": payload.get("request_id"),
                }
            )
        )

    print(json.dumps({"cases": len(cases), "failed": failed, "passed": len(cases) - failed}))
    return 1 if failed else 0


def request_case(api_url: str, session_id: str, case: dict[str, Any]) -> dict[str, Any]:
    request_id = f"route-eval-{uuid.uuid4().hex}"
    body = json.dumps(
        {
            "prompt": case["prompt"],
            "session_id": session_id,
            "request_id": request_id,
            "parent_request_id": None,
            "scope": {"route_id": None, "week_end": None},
        }
    ).encode()
    request = urllib.request.Request(
        f"{api_url.rstrip('/')}/v1/query",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"Eval request failed with HTTP {exc.code}: {detail}") from exc


def response_route_groups(payload: dict[str, Any]) -> set[str]:
    groups: set[str] = set()
    for table in payload.get("tables") or []:
        for row in table.get("rows") or []:
            if isinstance(row, dict) and row.get("route_group"):
                groups.add(str(row["route_group"]))
    return groups


def validate_case(case: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    groups = response_route_groups(payload)
    required = set(case.get("required_route_groups") or [])
    forbidden = set(case.get("forbidden_route_groups") or [])
    errors = []
    if not required.issubset(groups):
        errors.append(f"missing route groups: {sorted(required - groups)}")
    if groups & forbidden:
        errors.append(f"forbidden route groups returned: {sorted(groups & forbidden)}")
    phrases = [value.lower() for value in case.get("answer_contains_any") or []]
    answer = str(payload.get("answer") or "").lower()
    if phrases and not any(phrase in answer for phrase in phrases):
        errors.append("answer did not explain the service-group separation")
    return errors


if __name__ == "__main__":
    sys.exit(main())
