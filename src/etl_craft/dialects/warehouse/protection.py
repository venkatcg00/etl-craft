"""SQL for atomic warehouse protection and independent column provenance."""

from __future__ import annotations

from dataclasses import dataclass

from etl_craft.core.enums import ProtectionEvent


@dataclass(frozen=True)
class ProtectionSql:
    """One target's evidence names and typed fingerprint expressions, shared by all writers."""

    target: str
    journal: str
    snapshot: str
    key_hash: str
    cell_hashes: dict[str, str]
    row_hash: str

    def ddl(self) -> tuple[str, str]:
        """Keep evidence append-only; the snapshot contains only row IDs and fingerprints."""
        return (
            f"CREATE TABLE {self.journal} (revision BIGINT NOT NULL, key_hash VARCHAR(32), "
            "column_name VARCHAR(255), value_hash VARCHAR(32), locked BOOLEAN NOT NULL, "
            "writer VARCHAR(128), writer_kind VARCHAR(16), actor VARCHAR(128) NOT NULL, "
            "actor_kind VARCHAR(16) NOT NULL, happened_at TIMESTAMP WITH TIME ZONE NOT NULL, "
            "request_id VARCHAR(128) NOT NULL, reason VARCHAR(4096) NOT NULL, "
            "event_kind VARCHAR(16) NOT NULL, contract VARCHAR(4096), "
            "UNIQUE(revision, key_hash, column_name))",
            f"CREATE TABLE {self.snapshot} "
            "(row_id BIGINT PRIMARY KEY, row_hash VARCHAR(32) NOT NULL)",
        )

    def state(self) -> str:
        """Read the latest evidence for each business key and column, including row locks."""
        return (
            "SELECT key_hash, column_name, value_hash, locked, writer, writer_kind, "
            "actor, actor_kind, happened_at, request_id, reason, event_kind, revision FROM "
            "(SELECT *, ROW_NUMBER() OVER (PARTITION BY key_hash, column_name "
            f"ORDER BY revision DESC) AS etl_rank FROM {self.journal} "
            "WHERE key_hash IS NOT NULL) e WHERE etl_rank = 1"
        )

    def rows(self) -> str:
        """Fingerprint every physical version so outside history edits are detected too."""
        return f"SELECT t.ROW_ID AS row_id, {self.row_hash} AS row_hash FROM {self.target} t"

    def drift(self) -> str:
        """Find insertions, updates or deletions absent from the committed evidence."""
        return (
            f"SELECT COUNT(*) FROM ({self.rows()}) t FULL OUTER JOIN {self.snapshot} s "
            "ON t.row_id = s.row_id WHERE t.row_id IS NULL OR s.row_id IS NULL "
            "OR t.row_hash IS DISTINCT FROM s.row_hash"
        )

    def current(self) -> str:
        """Use the latest physical version, including a deleted key awaiting revival."""
        return (
            f"SELECT * FROM (SELECT t.*, ROW_NUMBER() OVER (PARTITION BY {self.key_hash} "
            f"ORDER BY t.ROW_ID DESC) AS etl_rank FROM {self.target} t) v WHERE etl_rank = 1"
        )

    def cells(self) -> str:
        """Project typed fingerprints of the current business values, never their contents."""
        return " UNION ALL ".join(
            f"SELECT {self.key_hash} AS key_hash, '{column}' AS column_name, "
            f"{hashed} AS value_hash FROM ({self.current()}) t"
            for column, hashed in self.cell_hashes.items()
        )

    def insert_event(self, body: str) -> str:
        """Append one revision's evidence with the actual request identity."""
        return (
            f"INSERT INTO {self.journal} (revision, key_hash, column_name, value_hash, locked, "
            "writer, writer_kind, actor, actor_kind, happened_at, request_id, reason, "
            "event_kind, contract) " + body
        )

    def baseline(self) -> str:
        """Existing values have explicitly unknown authors; actor strings are not proof."""
        return self.insert_event(
            "SELECT :revision, c.key_hash, c.column_name, c.value_hash, FALSE, NULL, NULL, "
            ":actor, :actor_kind, :now, :request_id, :reason, :event, NULL "
            f"FROM ({self.cells()}) c"
        )

    def changes(self, cells: str, state: str) -> str:
        """Record actual changes and preserve previous locks on automated writes."""
        return self.insert_event(
            "SELECT :revision, COALESCE(c.key_hash, p.key_hash), "
            "COALESCE(c.column_name, p.column_name), c.value_hash, "
            "COALESCE(p.locked, FALSE) OR (:human AND p.value_hash IS NOT NULL), "
            ":actor, :actor_kind, :actor, :actor_kind, :now, :request_id, :reason, "
            f"CASE WHEN c.key_hash IS NULL THEN '{ProtectionEvent.DELETE}' "
            f"ELSE '{ProtectionEvent.WRITE}' END, NULL "
            f"FROM {cells} c FULL OUTER JOIN {state} p "
            "ON c.key_hash = p.key_hash AND c.column_name = p.column_name "
            "WHERE (p.column_name IS NULL OR p.column_name <> '*') "
            "AND c.value_hash IS DISTINCT FROM p.value_hash"
        )

    def inserted_locks(self, cells: str, state: str) -> str:
        """Protect the entire business row on a deliberate human insert."""
        return self.insert_event(
            "SELECT DISTINCT :revision, c.key_hash, '*', NULL, TRUE, :actor, :actor_kind, "
            ":actor, :actor_kind, :now, :request_id, :reason, :event, NULL "
            f"FROM {cells} c WHERE NOT EXISTS (SELECT 1 FROM {state} p "
            "WHERE p.key_hash = c.key_hash AND p.value_hash IS NOT NULL)"
        )

    def protect_stage(self, stage: str, columns: tuple[str, ...], *, delete: bool) -> str:
        """Exclude protected rows and overlay locked values before hashing the source."""
        key = self.key_hash.replace("t.", "s.")
        state = self.state()
        lock = f"EXISTS (SELECT 1 FROM ({state}) p WHERE p.key_hash = {key} AND p.locked"
        selection = ", ".join(f"s.{column}" for column in columns)
        if not delete:
            selection = ", ".join(
                f"CASE WHEN {lock} AND p.column_name = '{column}') "
                f"THEN t.{column} ELSE s.{column} END AS {column}"
                for column in columns
            )
        join = "" if delete else f" LEFT JOIN ({self.current()}) t ON {self.key_hash} = {key}"
        row_lock = lock + (")" if delete else " AND p.column_name = '*')")
        return f"SELECT {selection} FROM {stage} s{join} WHERE NOT {row_lock}"

    def lock_change(self, predicate: str) -> str:
        """Unlock keeps value-writer attribution; the operation actor is recorded separately."""
        return self.insert_event(
            "SELECT :revision, key_hash, column_name, value_hash, :locked, writer, writer_kind, "
            ":actor, :actor_kind, :now, :request_id, :reason, :event, NULL "
            f"FROM ({self.state()}) p WHERE {predicate}"
        )

    def prevented(self, stage: str, columns: tuple[str, ...], *, delete: bool) -> str:
        """Count attempted protected changes separately from unchanged input."""
        key = self.key_hash.replace("t.", "s.")
        join = f"FROM {stage} s JOIN ({self.current()}) t ON {self.key_hash} = {key} "
        state = self.state()
        if delete:
            return (
                f"SELECT DISTINCT {key} AS key_hash, p.column_name {join} "
                f"JOIN ({state}) p ON p.key_hash = {key} WHERE p.locked"
            )
        return " UNION ALL ".join(
            f"SELECT {key} AS key_hash, '{column}' AS column_name {join} "
            f"WHERE s.{column} IS DISTINCT FROM t.{column} AND EXISTS "
            f"(SELECT 1 FROM ({state}) p WHERE p.key_hash = {key} AND p.locked "
            f"AND p.column_name IN ('*', '{column}'))"
            for column in columns
        )

    def invalid_keys(self, keys: tuple[str, ...], *, scd2: bool) -> str:
        """Reject ambiguous current ownership and NULL business identities before activation."""
        nulls = " OR ".join(f"{key} IS NULL" for key in keys)
        active = "WHERE ACTIVE_FLAG = 'Y'" if scd2 else ""
        return (
            f"SELECT (SELECT COUNT(*) FROM {self.target} WHERE {nulls}) + "
            f"(SELECT COUNT(*) FROM (SELECT {', '.join(keys)} FROM {self.target} {active} "
            f"GROUP BY {', '.join(keys)} HAVING COUNT(*) > 1) d)"
        )
