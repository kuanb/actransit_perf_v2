from __future__ import annotations

import os
from typing import Any

from vertex_open_model import VertexOpenModelSession


class VertexMistralModelProvider:
    def __init__(self, project_id: str, location: str, model: str) -> None:
        self.project_id = project_id
        self.location = location
        self.model = model
        self.timeout_seconds = float(
            os.environ.get("ANALYSIS_MODEL_TIMEOUT_SECONDS", "30")
        )

    def start_session(
        self,
        system_instruction: str,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]],
    ) -> VertexOpenModelSession:
        from google import auth
        from google.auth.transport.requests import AuthorizedSession

        credentials, _ = auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        authorized_session = AuthorizedSession(credentials)
        return VertexOpenModelSession(
            authorized_session,
            self.endpoint_url(),
            self.model,
            system_instruction,
            messages,
            tools,
            self.timeout_seconds,
        )

    def endpoint_url(self) -> str:
        return (
            f"https://{self.location}-aiplatform.googleapis.com/v1/projects/"
            f"{self.project_id}/locations/{self.location}/publishers/mistralai/"
            f"models/{self.model}:rawPredict"
        )
