"""Load intervention policy once at the shared operation boundary."""

from etl_craft.core.governance import ProjectPolicy
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext


def project_policy(ctx: OperationContext, request: object) -> ProjectPolicy:
    """Deployment actions have no project policy and keep mandatory admin reasons."""
    if ctx.project_id is None or not hasattr(request, "reason"):
        return ProjectPolicy()
    with ctx.engine.connect() as conn:
        row = conn.execute(
            statement(conn, "project_governance"), {"project_id": ctx.project_id}
        ).one()
    return ProjectPolicy(ctx.project, row.environment_type, row.governance)
