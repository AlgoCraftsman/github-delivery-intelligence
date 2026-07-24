"""Versioned GitHub event envelope models."""

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints

NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
RepositoryFullName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, pattern=r"^[^/\s]+/[^/\s]+$"),
]


class GitHubEventName(StrEnum):
    """Webhook event families supported by the MVP contract."""

    PULL_REQUEST = "pull_request"
    PULL_REQUEST_REVIEW = "pull_request_review"
    WORKFLOW_RUN = "workflow_run"
    DEPLOYMENT = "deployment"
    DEPLOYMENT_STATUS = "deployment_status"


class _Installation(BaseModel):
    id: int = Field(gt=0)


class _Repository(BaseModel):
    id: int = Field(gt=0)
    full_name: RepositoryFullName


class _WebhookPayload(BaseModel):
    action: NonEmptyString
    installation: _Installation
    repository: _Repository


class GitHubEventEnvelope(BaseModel):
    """Stable contract published by the receiver after request validation."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    delivery_id: NonEmptyString
    event_name: GitHubEventName
    action: NonEmptyString
    installation_id: int = Field(gt=0)
    repository_id: int = Field(gt=0)
    repository_full_name: RepositoryFullName
    received_at: AwareDatetime
    payload: dict[str, Any]

    @classmethod
    def from_webhook(
        cls,
        *,
        delivery_id: str,
        event_name: GitHubEventName,
        received_at: datetime,
        payload: dict[str, Any],
    ) -> "GitHubEventEnvelope":
        """Build an envelope while retaining the complete original JSON object."""

        common = _WebhookPayload.model_validate(payload)
        return cls(
            delivery_id=delivery_id,
            event_name=event_name,
            action=common.action,
            installation_id=common.installation.id,
            repository_id=common.repository.id,
            repository_full_name=common.repository.full_name,
            received_at=received_at,
            payload=payload,
        )


class WebhookReceipt(BaseModel):
    """Minimal response that does not echo a private webhook payload."""

    status: Literal["accepted"] = "accepted"
    delivery_id: NonEmptyString
