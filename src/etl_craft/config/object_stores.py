"""Named S3-compatible credentials and the deployment's artifact location."""

from dataclasses import replace
from pathlib import Path
from urllib.parse import SplitResult, urlsplit

from etl_craft.config.loader_helpers import mapping, reject_unknown
from etl_craft.config.model import ConnectorConfig
from etl_craft.config.object_models import Artifacts, ObjectStore
from etl_craft.config.resolve import Resolver
from etl_craft.core.enums import ObjectStoreAuth, ObjectUrlStyle
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.text import mask_arguments

STORE_KEYS = frozenset(
    {"Endpoint", "Region", "Url_style", "Auth_mode", "Access_key_id", "Secret_access_key"}
)
ARTIFACT_KEYS = frozenset({"Store", "Profile"})


def parse_stores(raw: dict[str, object], path: Path, resolver: Resolver) -> dict[str, ObjectStore]:
    """Validate every named profile without resolving credentials into configuration."""
    result = {}
    for name, value in mapping(raw, "Object_stores", path, required=False).items():
        where = f"Object_stores.{name}"
        if not isinstance(value, dict):
            raise ConfigurationError(
                f"{path}: {where}={value!r}; expected a profile mapping; correct it"
            )
        reject_unknown(value, STORE_KEYS, where, path)
        try:
            mode = ObjectStoreAuth(value.get("Auth_mode", ObjectStoreAuth.DEFAULT))
            style = ObjectUrlStyle(value.get("Url_style", ObjectUrlStyle.VIRTUAL))
        except ValueError as error:
            raise ConfigurationError(
                f"{path}: {where} Auth_mode/Url_style={mask_arguments(value)!r}; "
                "expected KEYS/DEFAULT and "
                "PATH/VIRTUAL; choose the documented upper-case values"
            ) from error
        endpoint = resolver.text(value.get("Endpoint"), f"{where}.Endpoint")
        if endpoint:
            parsed = _address(endpoint, f"{where}.Endpoint", path)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.hostname
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
            ):
                raise ConfigurationError(
                    f"{path}: {where}.Endpoint is invalid; expected an HTTP(S) origin without "
                    f"credentials; correct the endpoint"
                )
        keys = [value.get("Access_key_id"), value.get("Secret_access_key")]
        if mode == ObjectStoreAuth.DEFAULT and any(v is not None for v in keys):
            raise ConfigurationError(
                f"{path}: {where} uses DEFAULT with key fields; expected the host credential "
                f"chain; remove the key fields"
            )
        names: list[str | None] = [None, None]
        if mode == ObjectStoreAuth.KEYS:
            names = [
                resolver.secret_reference(v, f"{where}.{key}")
                for v, key in zip(keys, ("Access_key_id", "Secret_access_key"), strict=True)
            ]
        if mode == ObjectStoreAuth.KEYS and any(v is None for v in names):
            raise ConfigurationError(
                f"{path}: {where} uses KEYS without both credential names; expected Access_key_id "
                "and Secret_access_key variables; declare both"
            )
        result[name] = ObjectStore(
            name,
            endpoint,
            resolver.text(value.get("Region"), f"{where}.Region"),
            style,
            mode,
            *names,
        )
    return result


def parse_artifacts(
    raw: dict[str, object], stores: dict[str, ObjectStore], path: Path
) -> Artifacts:
    """S3 locations name an explicit profile; file locations are absolute local paths."""
    values = mapping(raw, "Artifacts", path, required=False)
    reject_unknown(values, ARTIFACT_KEYS, "Artifacts", path)
    store, profile = values.get("Store"), values.get("Profile")
    if store is None:
        if profile is not None:
            raise ConfigurationError(
                f"{path}: Artifacts.Profile={profile!r} without Store; expected a store "
                f"location; set Store or remove Profile"
            )
        return Artifacts()
    if not isinstance(store, str) or (profile is not None and not isinstance(profile, str)):
        raise ConfigurationError(
            f"{path}: Artifacts={mask_arguments(values)!r}; expected text Store and Profile; "
            "correct the fields"
        )
    parsed = _address(store, "Artifacts.Store", path)
    valid = not (parsed.query or parsed.fragment or parsed.username or parsed.password)
    if parsed.scheme == "file":
        valid = valid and not parsed.netloc and Path(parsed.path).is_absolute() and profile is None
    elif parsed.scheme == "s3":
        valid = valid and bool(parsed.netloc) and profile in stores
    else:
        valid = False
    if not valid:
        address = mask_arguments({"store": store})["store"]
        raise ConfigurationError(
            f"{path}: Artifacts.Store={address!r}, Profile={profile!r}; expected "
            f"file:///absolute/path or s3://bucket/prefix with an Object_stores profile; "
            f"correct the location/profile"
        )
    return Artifacts(store if store == "file:///" else store.rstrip("/"), profile)


def with_artifacts(
    config: ConnectorConfig, raw: dict[str, object], path: Path, resolver: Resolver
) -> ConnectorConfig:
    """Attach object stores after the connection profiles have been validated."""
    stores = parse_stores(raw, path, resolver)
    return replace(config, object_stores=stores, artifacts=parse_artifacts(raw, stores, path))


def _address(value: str, where: str, path: Path) -> SplitResult:
    """Reject malformed URL syntax without exposing embedded credentials."""
    try:
        return urlsplit(value)
    except ValueError:
        address = mask_arguments({"address": value})["address"]
        raise ConfigurationError(
            f"{path}: {where}={address!r}; expected a valid URL; correct its address"
        ) from None
