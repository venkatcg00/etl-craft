"""Safely stream an existing whole-project catalog without generating or filtering files."""

import json
from collections.abc import Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO
from urllib.parse import unquote

from etl_craft.core.errors import (
    ConfigurationError,
    PermissionDeniedError,
    ResourceNotFoundError,
    UsageError,
)
from etl_craft.core.file_bytes import open_regular, parent_directory
from etl_craft.engine.queries import statement
from etl_craft.services.catalog.storage import MARKER, site_lock
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.documents import docs_folder
from etl_craft.services.operations.permissions import pipeline_access, require_project
from etl_craft.services.operations.requests import Request

CONTENT_TYPES = {
    ".svg": "image/svg+xml",
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".json": "application/json",
}


@dataclass(frozen=True, kw_only=True)
class CatalogInput(Request):
    """A generated page or asset relative to the selected project's catalog."""

    path: str = "index.html"

    def __post_init__(self) -> None:
        """Reject encoded and alternate-platform paths before touching the filesystem."""
        parts = self.path.split("/")
        if (
            not self.path
            or len(self.path) > 2048
            or unquote(self.path) != self.path
            or any(char in self.path for char in ("%", "\\", ":", "?", "#", "\x00"))
            or PurePosixPath(self.path).is_absolute()
            or any(not part or part.startswith(".") for part in parts)
            or PurePosixPath(self.path).suffix not in CONTENT_TYPES
        ):
            raise UsageError(
                f"catalog path={self.path!r}; expected a relative generated HTML, CSS, JS, JSON or "
                "SVG file, without traversal, drive letters or encoding; use a catalog link"
            )


@dataclass
class CatalogPage(Iterator[bytes]):
    """A regular-file descriptor owned until EOF or explicit close."""

    content_type: str
    stream: BinaryIO
    resources: ExitStack

    def __iter__(self) -> Iterator[bytes]:
        """Keep the descriptor across bounded chunk reads."""
        return self

    def __next__(self) -> bytes:
        """Close on EOF; a consumer that stops early calls close."""
        if self.stream.closed:
            raise StopIteration
        try:
            chunk = self.stream.read(65536)
        except OSError:
            self.close()
            raise
        if chunk:
            return chunk
        self.close()
        raise StopIteration

    def close(self) -> None:
        """Release all descriptor-relative file handles."""
        self.resources.close()


def _folder(ctx: OperationContext) -> Path:
    """Reuse an explicit successful generation output, otherwise the existing configured default."""
    with ctx.engine.connect() as conn:
        publication = conn.execute(
            statement(conn, "catalog_publication"), {"project_id": ctx.project_id}
        ).scalar_one_or_none()
    if publication is not None:
        fields = json.loads(publication) if isinstance(publication, str) else publication
        output = fields.get("output")
        if output:
            folder = Path(output)
            if not folder.is_absolute():
                raise ResourceNotFoundError(
                    f"project {ctx.project!r}: recorded catalog output is relative; expected "
                    "a server-readable absolute generation output; run generate-docs --output PATH "
                    "with an absolute path on the serving host"
                )
            return folder
    return docs_folder(ctx.config)


def read_catalog(ctx: OperationContext, request: CatalogInput) -> CatalogPage:
    """Require whole-project visibility, validate its marker, then open without following links."""
    with ctx.engine.connect() as conn:
        require_project(conn, ctx)
        access = pipeline_access(conn, ctx)
        if any(role is None for role in access.roles.values()):
            raise PermissionDeniedError(
                f"project {ctx.project!r}: generated catalog lists every pipeline, but your "
                "access is limited to some pipelines; ask a project manager for whole-project "
                "view access or use authorized pipeline views"
            )
    folder = _folder(ctx)
    if not folder.is_dir():
        raise ResourceNotFoundError(
            f"project {ctx.project!r}: catalog is not generated; "
            "run `etl-craft generate-docs` first"
        )
    resources = ExitStack()
    try:
        with site_lock(folder, shared=True), parent_directory(folder, "index.html") as descriptor:
            with open_regular(folder, MARKER, root_descriptor=descriptor) as marker:
                value = marker.read(4097)
                identity = json.loads(value)
                if (
                    len(value) > 4096
                    or not isinstance(identity, dict)
                    or identity.get("project") != ctx.project
                ):
                    raise ResourceNotFoundError(
                        f"project {ctx.project!r}: catalog belongs to another project or has an "
                        "invalid generation marker; run generate-docs for the selected project"
                    )
            path = folder / request.path
            if not path.resolve().is_relative_to(folder.resolve()):
                raise UsageError("catalog path escapes its generated folder; use a catalog link")
            stream = resources.enter_context(
                open_regular(folder, request.path, root_descriptor=descriptor)
            )
        return CatalogPage(CONTENT_TYPES[PurePosixPath(request.path).suffix], stream, resources)
    except (ConfigurationError, OSError, ValueError, RuntimeError) as error:
        resources.close()
        raise ResourceNotFoundError(
            f"project {ctx.project!r}, catalog path={request.path!r}: not generated or not a safe "
            "regular file; run generate-docs and restore readable files without symlinks"
        ) from error
    except Exception:
        # Boundary: close transferred descriptors before propagating a domain failure.
        resources.close()
        raise
