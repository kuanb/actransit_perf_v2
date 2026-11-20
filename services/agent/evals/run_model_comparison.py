from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


EVAL_DIR = Path(__file__).resolve().parent
AGENT_DIR = EVAL_DIR.parent
sys.path.insert(0, str(AGENT_DIR))
sys.path.insert(0, str(EVAL_DIR))

from model_eval import (  # noqa: E402
    GeminiRunner,
    MistralRunner,
    OpenModelRunner,
    load_tool_catalog,
    parse_model_spec,
    score_turn,
    summarize,
)


DEFAULT_MODELS = ",".join(
    [
        "gemini:gemini-3.5-flash-lite",
        "open:google/gemma-4-26b-a4b-it-maas",
        "open:openai/gpt-oss-120b-maas",
        "open:deepseek-ai/deepseek-v4.1-flash-maas",
        "open:meta/llama-4-scout-17b-16e-instruct-maas@us-east5",
        "mistral:mistral-small-2503@us-central1",
        "open:xai/grok-4.7",
    ]
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare Vertex models on deterministic AC Transit agent cases.")
    parser.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT", "transit-203605"))
    parser.add_argument("--location", default="global")
    parser.add_argument("--models", default=DEFAULT_MODELS)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--cases", default=str(EVAL_DIR / "model_comparison_cases.json"))
    parser.add_argument("--case", action="append", dest="case_ids", help="Run only this case ID; repeat for more cases")
    parser.add_argument("--output-dir", default="output/model-eval")
    parser.add_argument("--min-pass-rate", type=float, default=0.0)
    parser.add_argument("--min-critical-pass-rate", type=float, default=0.0)
    parser.add_argument("--request-timeout", type=float, default=90.0)
    parser.add_argument("--provider-retries", type=int, default=1)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be at least 1")
    if args.request_timeout <= 0:
        parser.error("--request-timeout must be positive")
    if args.provider_retries < 0:
        parser.error("--provider-retries cannot be negative")
    os.environ["ANALYSIS_MODEL_TIMEOUT_SECONDS"] = str(args.request_timeout)

    suite = json.loads(Path(args.cases).read_text())
    selected_cases = [case for case in suite["cases"] if not args.case_ids or case["id"] in args.case_ids]
    if args.case_ids and len(selected_cases) != len(set(args.case_ids)):
        parser.error("Every --case value must name a case in the suite")
    evaluation_date = date.fromisoformat(suite["evaluation_date"])
    specs = [parse_model_spec(value.strip(), args.location) for value in args.models.split(",") if value.strip()]
    catalog = load_tool_catalog()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    results_path = output_dir / "results.jsonl"
    results_path.write_text("")
    records: list[dict[str, Any]] = []

    for spec in specs:
        try:
            runner_class = {
                "gemini": GeminiRunner,
                "open": OpenModelRunner,
                "mistral": MistralRunner,
            }[spec.provider]
            runner = runner_class(args.project, spec, catalog, evaluation_date)
            initialization_error = None
        except Exception as exc:
            runner = None
            initialization_error = f"{type(exc).__name__}: {exc}"
        provider_error = initialization_error
        for repeat in range(1, args.repeats + 1):
            for case in selected_cases:
                conversation: list[dict[str, Any]] = []
                for turn_index, turn in enumerate(case["turns"], start=1):
                    error = provider_error
                    failure_latency = 0.0
                    if runner is not None and provider_error is None:
                        started = time.monotonic()
                        for attempt in range(args.provider_retries + 1):
                            try:
                                actual = runner.run_turn(turn["prompt"], case.get("scope") or {}, conversation)
                                error = None
                                break
                            except Exception as exc:
                                error = f"{type(exc).__name__}: {exc}"
                                if attempt == args.provider_retries:
                                    provider_error = error
                                    failure_latency = time.monotonic() - started
                    if error:
                        actual = {
                            "answer": "",
                            "maps": [],
                            "charts": [],
                            "tables": [],
                            "timeframes": [],
                            "trace": [],
                            "usage": {"input_tokens": 0, "output_tokens": 0, "cached_tokens": 0},
                            "latency_seconds": failure_latency,
                        }
                    scores = score_turn(turn, actual)
                    if error:
                        scores = {
                            "tool_selection": 0.0,
                            "arguments": 0.0,
                            "answer_policy": 0.0,
                            "artifacts": 0.0,
                            "efficiency": 0.0,
                            "score": 0.0,
                            "passed": False,
                        }
                    record = {
                        "model": spec.label,
                        "model_id": spec.model,
                        "provider": spec.provider,
                        "location": spec.location,
                        "repeat": repeat,
                        "case": case["id"],
                        "turn": turn_index,
                        "critical": bool(case.get("critical")),
                        "prompt": turn["prompt"],
                        "expected_calls": turn.get("expected_calls") or [],
                        "answer": actual["answer"],
                        "trace": actual["trace"],
                        "timeframes": actual.get("timeframes") or [],
                        "returned_artifacts": {
                            "map_count": len(actual.get("maps") or []),
                            "chart_titles": [item.get("title") for item in actual.get("charts") or []],
                            "table_titles": [item.get("title") for item in actual.get("tables") or []],
                        },
                        "usage": actual["usage"],
                        "latency_seconds": actual["latency_seconds"],
                        "scores": scores,
                        "error": error,
                    }
                    records.append(record)
                    with results_path.open("a") as handle:
                        handle.write(json.dumps(record) + "\n")
                    print(json.dumps({"model": spec.label, "repeat": repeat, "case": case["id"], "turn": turn_index, "passed": scores["passed"], "score": round(scores["score"], 3), "error": error}), flush=True)
                    conversation.append({"question": turn["prompt"], "answer": actual["answer"]})
        if runner is not None:
            runner.close()

    summaries = [summarize(spec, [record for record in records if record["model_id"] == spec.model]) for spec in specs]
    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "project": args.project,
        "evaluation_date": suite["evaluation_date"],
        "repeats": args.repeats,
        "case_count": len(selected_cases),
        "turn_count": sum(len(case["turns"]) for case in selected_cases),
        "pricing_as_of": "2026-11-19",
        "request_timeout_seconds": args.request_timeout,
        "provider_retries": args.provider_retries,
        "summaries": summaries,
    }
    (output_dir / "summary.json").write_text(json.dumps(metadata, indent=2) + "\n")
    with (output_dir / "comparison.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summaries[0]) if summaries else [])
        writer.writeheader()
        writer.writerows(summaries)

    table = render_table(summaries)
    print("\n" + table)
    github_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if github_summary:
        with Path(github_summary).open("a") as handle:
            handle.write("## AC Transit model evaluation\n\n" + table + "\n")
    failed = any(item["pass_rate"] < args.min_pass_rate or item["critical_pass_rate"] < args.min_critical_pass_rate or item["errors"] for item in summaries)
    return 1 if failed else 0


def render_table(summaries: list[dict[str, Any]]) -> str:
    lines = [
        "| Model | Pass | Critical | Tool | Args | Completed | p95 latency | Cost/completed turn | Error/skip turns |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in summaries:
        lines.append(
            f"| {item['model']} | {item['pass_rate']:.1%} | {item['critical_pass_rate']:.1%} | "
            f"{item['tool_selection_accuracy']:.1%} | {item['argument_accuracy']:.1%} | "
            f"{item['completed_runs']}/{item['runs']} | {item['p95_latency_seconds']:.2f}s | "
            f"${item['estimated_cost_per_turn_usd']:.6f} | {item['errors']} |"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    sys.exit(main())
