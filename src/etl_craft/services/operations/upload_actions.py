"""Stewardship action contracts for user interfaces and API-only task hosts."""

from etl_craft.core.enums import ActionName, ActionScope, ApiRole, Audit, HttpMethod
from etl_craft.services.operations import agent_uploads, uploads
from etl_craft.services.operations.action_types import Action
from etl_craft.services.operations.requests import NoInput

UPLOAD_ACTIONS = (
    Action(
        ActionName.AGENT_UPLOAD_CONTENT,
        agent_uploads.content,
        agent_uploads.AgentUpload,
        None,
        ApiRole.AGENT,
        HttpMethod.GET,
        "/api/v1/agent/uploads/{upload_id}/content",
        streams=True,
        escalate=agent_uploads.required_role,
    ),
    Action(
        ActionName.UPLOAD,
        uploads.receive,
        uploads.UploadRequest,
        uploads.UploadSubmission,
        ApiRole.VIEWER,
        HttpMethod.POST,
        "/api/v1/uploads",
        Audit.BY_ACTION,
        scope=ActionScope.PIPELINE,
    ),
    Action(
        ActionName.UPLOADS,
        uploads.history,
        uploads.UploadRequest,
        uploads.UploadHistory,
        ApiRole.VIEWER,
        HttpMethod.GET,
        "/api/v1/uploads",
        scope=ActionScope.PIPELINE,
    ),
    Action(
        ActionName.UPLOAD_PIPELINES,
        uploads.eligible,
        NoInput,
        uploads.UploadPipelines,
        ApiRole.VIEWER,
        HttpMethod.GET,
        "/api/v1/uploads/pipelines",
        scope=ActionScope.PROJECT,
    ),
)
