from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from limiter import FirestoreRateLimiter, LimitConfig, RateLimitError
from orchestrator import AnalysisAgent, MCPGateway
from policy import eval_prompt_features, extend_conversation, follow_ups_remaining


logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("actransit-agent")
ROUTE_PATTERN = re.compile(r"^[A-Za-z0-9-]{1,16}$")


def structured_log(event: str, **fields: Any) -> None:
    print(json.dumps({"severity": "INFO", "event": event, **fields}, default=str), flush=True)


@dataclass(frozen=True)
class Settings:
    project_id: str
    mcp_url: str
    mcp_audience: str
    model: str
    vertex_location: str
    allowed_origins: list[str]
    enabled: bool
    max_prompt_chars: int
    max_response_bytes: int
    max_tool_calls: int
    max_follow_ups: int
    limits: LimitConfig

    @classmethod
    def from_env(cls) -> "Settings":
        project_id = os.environ.get("PROJECT_ID", "transit-203605")
        mcp_url = os.environ.get("MCP_URL", "http://localhost:8081/mcp")
        return cls(
            project_id=project_id,
            mcp_url=mcp_url,
            mcp_audience=os.environ.get("MCP_AUDIENCE", mcp_url.removesuffix("/mcp")),
            model=os.environ.get("GEMINI_MODEL", "gemini-3.5-flash-lite"),
            vertex_location=os.environ.get("VERTEX_LOCATION", "global"),
            allowed_origins=[
                value.strip()
                for value in os.environ.get(
                    "ALLOWED_ORIGINS",
                    "https://ac-transit.com,https://kuanb.github.io,http://localhost:8000,http://127.0.0.1:8000",
                ).split(",")
                if value.strip()
            ],
            enabled=os.environ.get("ANALYSIS_ENABLED", "false").lower() == "true",
            max_prompt_chars=int(os.environ.get("MAX_PROMPT_CHARS", "500")),
            max_response_bytes=int(os.environ.get("MAX_RESPONSE_BYTES", str(256 * 1024))),
            max_tool_calls=int(os.environ.get("MAX_TOOL_CALLS", "4")),
            max_follow_ups=int(os.environ.get("MAX_FOLLOW_UPS", "3")),
            limits=LimitConfig(
                cooldown_seconds=int(os.environ.get("COOLDOWN_SECONDS", "20")),
                max_active_queries=int(os.environ.get("MAX_ACTIVE_QUERIES", "2")),
                max_session_daily=int(os.environ.get("MAX_SESSION_DAILY", "10")),
                max_global_daily=int(os.environ.get("MAX_GLOBAL_DAILY", "60")),
                lease_seconds=int(os.environ.get("LEASE_SECONDS", "90")),
            ),
        )


class Scope(BaseModel):
    route_id: str | None = None
    week_end: str | None = None


class QueryBody(BaseModel):
    prompt: str = Field(min_length=3)
    session_id: str = Field(min_length=16, max_length=80, pattern=r"^[A-Za-z0-9-]+$")
    request_id: str = Field(min_length=16, max_length=80, pattern=r"^[A-Za-z0-9-]+$")
    parent_request_id: str | None = Field(
        default=None,
        min_length=16,
        max_length=80,
        pattern=r"^[A-Za-z0-9-]+$",
    )
    scope: Scope = Field(default_factory=Scope)


settings = Settings.from_env()
limiter = FirestoreRateLimiter(settings.project_id, settings.limits)
gateway = MCPGateway(settings.mcp_url, settings.mcp_audience)
agent = AnalysisAgent(
    settings.project_id,
    settings.vertex_location,
    settings.model,
    gateway,
    settings.max_tool_calls,
)

app = FastAPI(title="AC Transit analysis agent", docs_url=None, redoc_url=None, openapi_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)


@app.middleware("http")
async def response_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "actransit-agent"}


@app.get("/v1/status")
async def status() -> dict[str, Any]:
    state = await limiter.status()
    return {
        **state,
        "enabled": settings.enabled,
        "max_prompt_chars": settings.max_prompt_chars,
        "max_session_daily": settings.limits.max_session_daily,
        "max_follow_ups": settings.max_follow_ups,
    }


@app.post("/v1/query")
async def query(body: QueryBody) -> JSONResponse:
    if not settings.enabled:
        return JSONResponse({"code": "disabled", "message": "Public analysis is temporarily disabled."}, status_code=503)
    prompt = body.prompt.strip()
    if len(prompt) > settings.max_prompt_chars:
        return JSONResponse(
            {
                "code": "prompt_too_long",
                "message": f"Questions are limited to {settings.max_prompt_chars} characters.",
            },
            status_code=422,
        )
    route_id = body.scope.route_id.strip().upper() if body.scope.route_id else None
    if route_id and not ROUTE_PATTERN.fullmatch(route_id):
        return JSONResponse({"code": "invalid_route", "message": "The route scope is invalid."}, status_code=422)
    if body.scope.week_end:
        try:
            date.fromisoformat(body.scope.week_end)
        except ValueError:
            return JSONResponse(
                {"code": "invalid_week", "message": "The route-page week is invalid."},
                status_code=422,
            )

    cached = await limiter.get_cached(body.session_id, body.request_id)
    if cached:
        return JSONResponse(cached)

    conversation: list[dict[str, Any]] = []
    if body.parent_request_id:
        if body.parent_request_id == body.request_id:
            return JSONResponse(
                {"code": "invalid_parent", "message": "A follow-up must reference the preceding answer."},
                status_code=422,
            )
        stored_context = await limiter.get_context(body.session_id, body.parent_request_id)
        if stored_context is None:
            return JSONResponse(
                {
                    "code": "thread_unavailable",
                    "message": "This answer is no longer available for follow-up. Start a new question.",
                },
                status_code=409,
            )
        if len(stored_context) > settings.max_follow_ups:
            return JSONResponse(
                {
                    "code": "follow_up_limit",
                    "message": (
                        f"This answer has reached its {settings.max_follow_ups}-follow-up limit. "
                        "Start a new question."
                    ),
                },
                status_code=409,
            )
        conversation = stored_context

    try:
        reservation = await limiter.reserve(body.session_id, body.request_id)
    except RateLimitError as exc:
        response = JSONResponse(
            {
                "code": exc.code,
                "message": exc.message,
                "retry_after_seconds": exc.retry_after_seconds,
                "active_queries": exc.active_queries,
                "max_active_queries": settings.limits.max_active_queries,
            },
            status_code=429,
        )
        response.headers["Retry-After"] = str(exc.retry_after_seconds)
        return response

    try:
        result = await agent.analyze(
            prompt,
            route_id,
            body.scope.week_end if route_id else None,
            conversation,
        )
        eval_trace = result.pop("_eval_trace", [])
        structured_log(
            "analysis_eval_trace",
            policy_version="route-groups-v1",
            request_id=body.request_id,
            route_scope=route_id,
            prompt_length=len(prompt),
            prompt_features=eval_prompt_features(prompt),
            tool_calls=eval_trace,
        )
        updated_conversation = extend_conversation(
            conversation,
            prompt,
            result,
            settings.max_follow_ups,
        )
        response_body = {
            **result,
            "request_id": body.request_id,
            "next_allowed_at": reservation.next_allowed_at.isoformat(),
            "active_queries": reservation.active_queries,
            "max_active_queries": settings.limits.max_active_queries,
            "cooldown_seconds": settings.limits.cooldown_seconds,
            "follow_ups_remaining": follow_ups_remaining(
                updated_conversation,
                settings.max_follow_ups,
            ),
        }
        if len(json.dumps(response_body, default=str).encode()) > settings.max_response_bytes:
            raise ValueError("The structured response exceeded the public response-size limit")
        await limiter.store_result(
            body.session_id,
            body.request_id,
            response_body,
            updated_conversation,
        )
        return JSONResponse(response_body)
    except Exception as exc:
        logger.exception("analysis request failed", extra={"request_id": body.request_id})
        return JSONResponse(
            {
                "code": "analysis_failed",
                "message": "The analysis could not be completed. Please try again after the displayed cooldown.",
            },
            status_code=503,
        )
    finally:
        try:
            await limiter.release(body.session_id, body.request_id)
        except Exception:
            logger.exception("analysis reservation release failed", extra={"request_id": body.request_id})
