from __future__ import annotations

import ast
import asyncio
import math
import statistics
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

AGENT_DIR = Path(__file__).resolve().parents[1]
MCP_SERVER_PATH = Path(__file__).resolve().parents[2] / "data_mcp" / "server.py"


@dataclass(frozen=True)
class ModelSpec:
    provider: str
    model: str
    location: str
    input_price: float
    output_price: float
    cached_input_price: float

    @property
    def label(self) -> str:
        return self.model.split("/")[-1]


PRICING = {
    "gemini-3.5-flash-lite": (0.15, 1.25, 0.015),
    "gemini-3.1-flash-lite": (0.125, 0.75, 0.0125),
    "openai/gpt-oss-120b-maas": (0.09, 0.36, 0.09),
    "google/gemma-4-26b-a4b-it-maas": (0.15, 0.60, 0.015),
    "deepseek-ai/deepseek-v4.1-flash-maas": (0.30, 1.20, 0.03),
    "meta/llama-4-scout-17b-16e-instruct-maas": (0.25, 0.70, 0.25),
    "mistral-small-2503": (0.10, 0.30, 0.10),
    "xai/grok-4.7": (2.00, 6.00, 0.50),
}


def parse_model_spec(value: str, default_location: str) -> ModelSpec:
    provider, separator, remainder = value.partition(":")
    if not separator or provider not in {"gemini", "open", "mistral"}:
        raise ValueError(
            f"Invalid model {value!r}; use gemini:MODEL, open:PUBLISHER/MODEL, "
            "or mistral:MODEL"
        )
    model, at, location = remainder.rpartition("@")
    if not at:
        model = remainder
        location = default_location
    if model not in PRICING:
        raise ValueError(f"No price configured for {model}; add its per-million-token price to PRICING")
    input_price, output_price, cached_input_price = PRICING[model]
    return ModelSpec(provider, model, location, input_price, output_price, cached_input_price)


def load_tool_catalog(path: Path = MCP_SERVER_PATH) -> list[dict[str, Any]]:
    module = ast.parse(path.read_text())
    tools = []
    for node in module.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not any(_is_tool_decorator(item) for item in node.decorator_list):
            continue
        properties: dict[str, Any] = {}
        required: list[str] = []
        defaults = [None] * (len(node.args.args) - len(node.args.defaults)) + list(node.args.defaults)
        for argument, default in zip(node.args.args, defaults):
            properties[argument.arg] = _annotation_schema(argument.annotation)
            if default is None:
                required.append(argument.arg)
            else:
                value = ast.literal_eval(default)
                if value is not None:
                    properties[argument.arg]["default"] = value
        schema: dict[str, Any] = {
            "type": "object",
            "properties": properties,
            "additionalProperties": False,
        }
        if required:
            schema["required"] = required
        tools.append(
            {
                "name": node.name,
                "description": ast.get_docstring(node) or "",
                "input_schema": schema,
            }
        )
    return tools


def _is_tool_decorator(node: ast.expr) -> bool:
    target = node.func if isinstance(node, ast.Call) else node
    return isinstance(target, ast.Attribute) and target.attr == "tool"


def _annotation_schema(node: ast.expr | None) -> dict[str, Any]:
    if node is None:
        return {}
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        variants = [_annotation_schema(node.left), _annotation_schema(node.right)]
        non_null = [item for item in variants if item.get("type") != "null"]
        return non_null[0] if len(non_null) == 1 else {"anyOf": variants}
    if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name):
        if node.value.id == "Literal":
            values = node.slice.elts if isinstance(node.slice, ast.Tuple) else [node.slice]
            literals = [ast.literal_eval(item) for item in values]
            return {"type": _json_type(literals[0]), "enum": literals}
        if node.value.id == "list":
            return {"type": "array", "items": _annotation_schema(node.slice)}
    if isinstance(node, ast.Name):
        return {"type": {"str": "string", "int": "integer", "float": "number", "bool": "boolean", "None": "null"}.get(node.id, "object")}
    if isinstance(node, ast.Constant) and node.value is None:
        return {"type": "null"}
    return {}


def _json_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"


class FixtureGateway:
    def __init__(
        self,
        catalog: list[dict[str, Any]],
        evaluation_date: date,
    ) -> None:
        self.catalog = catalog
        self.evaluation_date = evaluation_date

    async def list_tools(self) -> list[dict[str, Any]]:
        return self.catalog

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        end_date = arguments.get("end_date") or (self.evaluation_date - timedelta(days=1)).isoformat()
        start_date = arguments.get("start_date") or (date.fromisoformat(end_date) - timedelta(days=6)).isoformat()
        timeframe = {"start_date": start_date, "end_date": end_date, "assumed": not arguments.get("start_date")}
        route = arguments.get("route_id") or "the system"
        result: dict[str, Any] = {
            "answer_context": f"Performance data is available for {route} from {start_date} through {end_date}.",
            "timeframe": timeframe,
            "maps": [],
            "charts": [],
            "tables": [],
            "sources": [{"kind": "eval_fixture"}],
            "warnings": [],
        }
        if name == "get_route_service_profile":
            result["answer_context"] += " It reports scheduled and Dropped / not observed trips by day and hour."
            result["charts"] = [{"type": "bar"}, {"type": "heatmap"}]
        elif name in {"get_route_delay_map", "get_route_segment_performance_map"}:
            result["maps"] = [{
                "title": f"Route {route}",
                "geojson": {
                    "type": "FeatureCollection",
                    "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [-122.27, 37.80]}, "properties": {"value": 1}}],
                },
            }]
            if name == "get_route_segment_performance_map":
                result["answer_context"] += " These are stop-to-stop sections and cannot isolate a particular turn or intersection."
                result["warnings"] = ["Stop-to-stop data cannot isolate a particular turn or intersection."]
        elif name == "get_metric_definition":
            result["answer_context"] = "On Time Service Delivered means an eligible scheduled stop was reached between one minute early and seven minutes late; the complement is outside rider window."
        elif name == "rank_routes":
            result["answer_context"] = f"Fixture {arguments.get('route_group', 'regular')} route ranking for {arguments.get('metric')}."
            if arguments.get("metric") == "on_time_service_delivered":
                result["answer_context"] += " The public metric is On Time Service Delivered."
            result["tables"] = [{"rows": [{"route_id": "1T", "route_group": arguments.get("route_group", "regular")}]}]
        elif name == "compare_routes":
            route_ids = arguments.get("route_ids") or []
            result["answer_context"] = f"Performance data is available to compare {', '.join(route_ids)}."
            result["tables"] = [{"rows": [{"route_id": route_id, "value": 2.5 + index} for index, route_id in enumerate(route_ids)]}]
        elif name in {"get_route_performance", "get_system_performance"}:
            result["answer_context"] += " On Time Service Delivered was 82% and Trip delivery was 96%; they are reported separately."
            result["tables"] = [{"rows": [{"metric": "on_time_service_delivered", "value": 82}, {"metric": "trip_delivery", "value": 96}]}]
        return result


class ManagedModelRunner:
    def __init__(
        self,
        provider_name: str,
        project_id: str,
        spec: ModelSpec,
        catalog: list[dict[str, Any]],
        evaluation_date: date,
    ) -> None:
        from model_provider import create_model_provider
        from orchestrator import AnalysisAgent

        provider = create_model_provider(
            provider_name,
            project_id,
            spec.location,
            spec.model,
        )
        self.agent = AnalysisAgent(
            provider,
            FixtureGateway(catalog, evaluation_date),
            max_tool_calls=4,
            today=evaluation_date,
        )
        self.loop = asyncio.new_event_loop()

    def run_turn(
        self,
        prompt: str,
        scope: dict[str, Any],
        conversation: list[dict[str, Any]],
    ) -> dict[str, Any]:
        started = time.monotonic()
        result = self.loop.run_until_complete(
            self.agent.analyze(
                prompt,
                scope.get("route_id"),
                scope.get("week_end"),
                conversation,
            )
        )
        trace = result.pop("_eval_trace")
        usage = result.pop("_model_usage")
        return {
            **result,
            "trace": trace,
            "usage": usage,
            "latency_seconds": time.monotonic() - started,
        }

    def close(self) -> None:
        self.loop.close()


class GeminiRunner(ManagedModelRunner):
    def __init__(
        self,
        project_id: str,
        spec: ModelSpec,
        catalog: list[dict[str, Any]],
        evaluation_date: date,
    ) -> None:
        super().__init__("gemini", project_id, spec, catalog, evaluation_date)


class OpenModelRunner(ManagedModelRunner):
    def __init__(
        self,
        project_id: str,
        spec: ModelSpec,
        catalog: list[dict[str, Any]],
        evaluation_date: date,
    ) -> None:
        super().__init__("vertex_open", project_id, spec, catalog, evaluation_date)


class MistralRunner(ManagedModelRunner):
    def __init__(
        self,
        project_id: str,
        spec: ModelSpec,
        catalog: list[dict[str, Any]],
        evaluation_date: date,
    ) -> None:
        super().__init__("vertex_mistral", project_id, spec, catalog, evaluation_date)


def score_turn(expected: dict[str, Any], actual: dict[str, Any]) -> dict[str, Any]:
    expected_calls = expected.get("expected_calls") or []
    actual_calls = [item for item in actual.get("trace") or [] if item.get("status") == "ok"]
    selection = float([item.get("name") for item in expected_calls] == [item.get("tool") for item in actual_calls])
    argument_parts = []
    for wanted, observed in zip(expected_calls, actual_calls):
        wanted_args = wanted.get("arguments") or {}
        observed_args = observed.get("arguments") or {}
        argument_parts.extend(float(observed_args.get(key) == value) for key, value in wanted_args.items())
    arguments = statistics.mean(argument_parts) if argument_parts else selection
    answer = str(actual.get("answer") or "").lower()
    contains = [value.lower() for value in expected.get("answer_contains_any") or []]
    forbidden = [value.lower() for value in expected.get("answer_forbidden") or []]
    policy = float((not contains or any(value in answer for value in contains)) and not any(value in answer for value in forbidden))
    artifact_parts = []
    artifacts = expected.get("required_artifacts") or {}
    if "maps" in artifacts:
        artifact_parts.append(float(len(actual.get("maps") or []) >= int(artifacts["maps"])))
    if "charts" in artifacts:
        chart_types = {str(item.get("type") or item.get("chart_type")) for item in actual.get("charts") or []}
        artifact_parts.append(float(set(artifacts["charts"]).issubset(chart_types)))
    artifact = statistics.mean(artifact_parts) if artifact_parts else 1.0
    efficiency = float(len(actual.get("trace") or []) <= int(expected.get("max_calls", 4)) and all(item.get("status") == "ok" for item in actual.get("trace") or []))
    weighted = 0.4 * selection + 0.25 * arguments + 0.2 * policy + 0.1 * artifact + 0.05 * efficiency
    return {
        "tool_selection": selection,
        "arguments": arguments,
        "answer_policy": policy,
        "artifacts": artifact,
        "efficiency": efficiency,
        "score": weighted,
        "passed": all(value == 1.0 for value in (selection, arguments, policy, artifact, efficiency)),
    }


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(quantile * len(ordered)) - 1)]


def summarize(model: ModelSpec, records: list[dict[str, Any]]) -> dict[str, Any]:
    completed_records = [record for record in records if not record.get("error")]
    scores = [record["scores"] for record in completed_records]
    usage = {key: sum(record["usage"][key] for record in records) for key in ("input_tokens", "output_tokens", "cached_tokens")}
    billable_input = max(0, usage["input_tokens"] - usage["cached_tokens"])
    cost = (
        billable_input * model.input_price
        + usage["cached_tokens"] * model.cached_input_price
        + usage["output_tokens"] * model.output_price
    ) / 1_000_000
    return {
        "model": model.label,
        "provider": model.provider,
        "location": model.location,
        "runs": len(records),
        "completed_runs": len(completed_records),
        "pass_rate": statistics.mean(item["passed"] for item in scores) if scores else 0.0,
        "critical_pass_rate": statistics.mean(record["scores"]["passed"] for record in completed_records if record["critical"]) if any(record["critical"] for record in completed_records) else 0.0,
        "mean_score": statistics.mean(item["score"] for item in scores) if scores else 0.0,
        "tool_selection_accuracy": statistics.mean(item["tool_selection"] for item in scores) if scores else 0.0,
        "argument_accuracy": statistics.mean(item["arguments"] for item in scores) if scores else 0.0,
        "answer_policy_accuracy": statistics.mean(item["answer_policy"] for item in scores) if scores else 0.0,
        "artifact_accuracy": statistics.mean(item["artifacts"] for item in scores) if scores else 0.0,
        "mean_latency_seconds": statistics.mean(record["latency_seconds"] for record in completed_records) if completed_records else 0.0,
        "p95_latency_seconds": percentile([record["latency_seconds"] for record in completed_records], 0.95),
        **usage,
        "estimated_cost_usd": cost,
        "estimated_cost_per_turn_usd": cost / len(completed_records) if completed_records else 0.0,
        "errors": sum(bool(record.get("error")) for record in records),
    }
