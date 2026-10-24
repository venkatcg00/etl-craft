"""Artifact locations and named object-store profiles."""

from dataclasses import dataclass

from etl_craft.core.enums import ObjectStoreAuth, ObjectUrlStyle


@dataclass(frozen=True)
class ObjectStore:
    """Credential names stay names until a store operation needs them."""

    name: str
    endpoint: str | None = None
    region: str | None = None
    url_style: ObjectUrlStyle = ObjectUrlStyle.VIRTUAL
    auth_mode: ObjectStoreAuth = ObjectStoreAuth.DEFAULT
    access_key_id: str | None = None
    secret_access_key: str | None = None


@dataclass(frozen=True)
class Artifacts:
    """An optional immutable bundle store, served to agents by the API."""

    store: str | None = None
    profile: str | None = None
