# Protecting human SCD edits

An authenticated human admin can correct or insert values in an existing SCD1 or SCD2
merge target through the shared service or HTTP API. The first successful edit enables
protection for that target. Ordinary tables keep their existing behavior and may omit actor
columns. Protected SCD targets require their full audit columns, including `CREATED_BY` and
`UPDATED_BY`.

## Writers and locks

- A human insert protects the whole business row. A human update protects each changed
  business value; `row_lock: true` also protects the whole row.
- A permitted human can edit a locked value. Its protection remains until an explicit unlock.
- ETL and authenticated service-account integrations create no human locks. They preserve
  existing locks, even when the pipeline was started by a person.
- Row locks prevent ETL updates and SCD2 closure. Any active value or row lock prevents both
  hard and soft deletion. Locks follow the ordered, typed `MERGE_KEY`, across versions.
- Column locks preserve the effective value while other columns change. Hashing and normal
  insert/update counts use the effective values. Input that differs only in a protected value
  creates no new SCD2 version. `PROTECTED_ROWS` and `PROTECTED_VALUES` in the task log report
  prevented changes separately from normal unchanged rows.

SCD1 keeps the original creation attribution. A deliberate SCD2 change closes the previous
version and creates a new attributed version, including edits outside `MERGE_COMPARE_COLUMNS`.
A deliberate edit has no execution run: its affected audit run/task IDs are NULL, and its
warehouse evidence records a unique request ID instead. ETL writes retain their execution IDs.

## HTTP requests

Sign in through the server's [password or SSO flow](../deploying/security.md) and use its
access token as `$ACCESS_TOKEN`. Substitute an existing pipeline, merge task, key and column:

```bash
curl --fail-with-body "$SERVER/api/v1/values" \
  -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"pipeline_code":"SALES","task_code":"customers","key":{"customer_id":42},"values":{"name":"Correct name"},"reason":"Verified with customer"}'

curl --fail-with-body "$SERVER/api/v1/values/provenance" \
  -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"pipeline_code":"SALES","task_code":"customers","key":{"customer_id":42}}'

curl --fail-with-body "$SERVER/api/v1/values/unlock" \
  -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"pipeline_code":"SALES","task_code":"customers","key":{"customer_id":42},"columns":["name"],"reason":"Return ownership to the feed"}'
```

Edits and unlocks require admin. Provenance reads require viewer. Service accounts with admin
may use the write endpoint for automated integrations, with their own identity and no automatic
locks; only USER admins may request a row lock or unlock. Scoped stewardship permissions and
file uploads are separate features.

Supply exactly the non-NULL key columns. Key and engine-managed columns cannot be edited.
An insert supplies every business column; an update retains omitted columns. Values are bound
parameters cast to the warehouse's declared types; use ISO strings for dates and timestamps.
Explicit NULL is a value, including when the ETL task uses `PRESERVE_TARGET`. An empty unlock
`columns` list releases every active lock for the key; `"*"` selects just its row lock. Unlock
changes no business data: the next merge may apply source values.

The response schema is `etl-craft/scd-values/1`, containing the target, protection-enabled
state, request ID, current per-column records and at most 100 recent events for the key.
`writer`/`writer_kind` describe the value's writer; `actor` describes the event's requester,
including an unlock. Pre-existing values have an explicitly unknown writer (`null`); actor
strings on the business table are never interpreted as proof. An untracked target returns
`enabled: false`. Evidence remains readable by key after deletion.

## Python service

Use an `OperationContext` holding the authenticated account and current role, as for other
[service operations](service-operations.md):

```python
from etl_craft.services.operations.values import (
    ValueEdit,
    ValueUnlock,
    provenance,
    unlock_values,
    write_values,
)

result = write_values(
    ctx,
    "SALES",
    "customers",
    ValueEdit(
        key={"customer_id": 42},
        values={"name": "Correct name"},
        reason="Verified with customer",
    ),
)
locks = provenance(ctx, "SALES", "customers", {"customer_id": 42})
unlock_values(
    ctx,
    "SALES",
    "customers",
    ValueUnlock(
        key={"customer_id": 42},
        reason="Return ownership to the feed",
        columns=("name",),
    ),
)
```

## Atomicity, outside writers and recovery

Protected writes currently require **native PostgreSQL or native DuckDB**. Other warehouses
refuse protected edits before changing data: they lack the implemented atomic data/evidence
contract. Existing unprotected SQL tasks on those warehouses retain their behavior.

Each enabled target has two bounded-name tables in its catalog/schema:
`etl_protection_<identity-hash>` is append-only lock/value-write evidence, and
`etl_fingerprints_<identity-hash>` holds only physical row IDs and committed fingerprints.
Evidence stores typed hashes of values, never copies of their contents. Treat these fingerprints
as sensitive metadata; restrict warehouse access accordingly. The evidence names are reserved
against SQL-task writes. No Engine DB migration, public setting or task parameter is added.

Edits, integrations, unlocks and ETL share the same target ownership lock. PostgreSQL also
locks the business table against outside DML for the transaction. DuckDB uses its existing
single-writer access and transactional conflict handling. Data, locks, provenance and
fingerprints commit or roll back together, so a retry after a process loss sees the committed
state. A failed first edit leaves protection disabled and the original table intact.

Every participating external writer must call the authenticated service. Arbitrary SQL,
ERP writes outside that service and scripts using their own warehouse connections are
unsupported. Before each protected operation, committed fingerprints are compared with every
physical version. An unobserved insert, update, deletion or history change stops the operation
with an explicit error; no human identity is guessed. This detects outside writes after they
happen; it cannot prevent a warehouse administrator bypassing the boundary or destroying both
evidence tables. Keep warehouse write credentials restricted to participating integrations.

Back up and restore the target and both evidence tables together. After detected outside
writes, restore a consistent backup and route the writer through the service. There is no
silent rebaseline or guessed recovery. Merge keys, compare columns and business shape must
continue to match the enabled contract. Replacement, append, drop and rehash operations refuse
enabled targets; audit upgrades refuse changes that would invalidate their evidence. Use SCD
merges for refreshes. Unlocking releases values, not the protection contract.

Fingerprint validation scans the enabled table, including closed SCD2 history, on each
operation; use this cost when choosing targets for protection. Ordinary unprotected targets
create no evidence tables.
