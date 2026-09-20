# `CFG_TASK_PARAMETERS` reference

Every task's behaviour beyond "which handler runs it" is configured with
`CFG_TASK_PARAMETERS` rows — one `PARAMETER_NAME` / `PARAMETER_VALUE` pair each.
That is deliberate: `CFG_TASKS` holds only what is true of *every* task regardless of
handler, so nothing carries columns most tasks never use.

**Multi-value convention:** any parameter holding more than one value is
pipe-separated (`a|b|c`). This is project-wide, with no exceptions.

## Every task

| Parameter | Required | Meaning |
|---|---|---|
| `SOURCE_OBJECT` | yes | `schema.table` (pipe-separated for several) this task reads. Declarative — used by `lineage`, checked by `validate`, not verified against the SQL. |
| `TARGET_OBJECT` | yes | `schema.table` this task writes. For `HANDLER='SQL'` this is also functional and must name exactly one table. |
| `TASK_TIMEOUT_SECONDS` | no | Wall-clock limit for this task, in seconds. Falls back to `[Execution] Task_timeout_seconds` (6 hours by default). `0` disables it. |
| `DOCUMENTATION` | no | Prose describing what this task does. Rendered on the generated documentation site and searchable there. Its **version is derived from the text**: `etl-craft docs-version` records a new version only when the wording genuinely changes, so a version can never silently disagree with what it describes. `etl-craft docs-version --pipeline_code X --task_code Y` shows the full history. |

## `HANDLER = 'SQL'`

The engine owns every write. You supply a bare, read-only `SELECT`; the engine wraps it in
whatever statement the declared action calls for and appends the audit columns that action
needs. Your `SELECT` must never project an engine-managed column itself.

| Parameter | Required | Meaning |
|---|---|---|
| `SQL_ACTION` | yes | One of `CREATE_TABLE`, `SETUP_TABLE`, `OVERWRITE_TABLE`, `SCD1_MERGE`, `SCD2_MERGE`, `DROP_TABLE`, `DELETE_ROWS`. |
| `SOURCE_SQL` | all but `DROP_TABLE` | The bare `SELECT`. May contain the literal token `$$pipeline_id`. |
| `PRIMARY_KEY` | no | Applied as `ADD PRIMARY KEY` when the engine creates the target, and re-applied after a schema evolution. Independent of `MERGE_KEY` — a merge target legitimately has both. |
| `MERGE_KEY` | SCD merges | Columns the merge matches on. |
| `MERGE_COMPARE_COLUMNS` | SCD merges | Columns hashed into `HASH_KEY` for change detection. |
| `MERGE_DEDUPE_ORDER` | no | An `ORDER BY` fragment (`updated_at DESC`) deciding which row wins when the source has duplicate `MERGE_KEY`s. Without it, duplicates are a clean failure *before* the target is touched — the engine will not invent an ordering you did not declare. |
| `SCHEMA_EVOLUTION` | no | `"true"` lets a new column in the source be added to the target. Never repairs missing audit columns. |
| `HARD_DELETE` | no | `DELETE_ROWS` only. `"true"` issues a real `DELETE`; anything else soft-deletes via `DELETE_FLAG='Y'`. |

### Actions

| Action | Effect | Audit columns appended |
|---|---|---|
| `CREATE_TABLE` | Replace the target entirely from the `SELECT`. | `PIPELINE_RUN_ID` |
| `SETUP_TABLE` | Create the target's *shape* only, zero rows. Infers audit columns from whichever sibling task actually writes it. | inferred |
| `OVERWRITE_TABLE` | Truncate and reinsert. | `+ UPDATE_DATE` |
| `SCD1_MERGE` | Update changed rows in place, insert new ones. | `+ HASH_KEY, CREATE_DATE, CREATED_BY, UPDATE_DATE, UPDATED_BY, DELETE_FLAG` |
| `SCD2_MERGE` | Deactivate changed rows, insert new versions. | the above `+ ACTIVE_FLAG` |
| `DROP_TABLE` | Drop the target. Refused unless a sibling `CREATE_TABLE` task for the same target has already succeeded in this run. | — |
| `DELETE_ROWS` | Soft or hard delete by `MERGE_KEY`. | — |

Every action except `DROP_TABLE` and `DELETE_ROWS` **creates the target if it does not
exist**, so a first run needs no separate setup task.

### `$$pipeline_id`

If your `SELECT` contains the literal token `$$pipeline_id`, the engine replaces it with
`pipeline_run_id = <this run>` for an incremental pipeline, or `1=1` for a full refresh.
If the token is absent the SQL is left **completely untouched** — the engine does not
guess where a filter belongs.

## `HANDLER = 'PYTHON'`

| Parameter | Required | Meaning |
|---|---|---|
| `SCRIPT_NAME` | yes | Script to run, as `<python> <script>`, in the current working directory. |
| `RETURN_VALUES` | yes | Pipe-separated variable names the script reports. Must include `INGESTION_COUNT` and `LATEST_OFFSET_UPDATE`. |

The engine does **not** inject the run id. Your script resolves `pipeline_run_id` itself.
It reports results by printing one trailing JSON line, e.g.:

```json
{"INGESTION_COUNT": 1200, "LATEST_OFFSET_UPDATE": "2026-09-20 00:00:00|timestamp"}
```

## `HANDLER = 'BUSINESS_RULES'`

Driven by `CFG_BUSINESS_RULES` rows rather than task parameters. Each rule's
`BUSINESS_RULE_SQL` is a **correlated condition**, not a standalone query — the target row
is aliased `t`. Rules sharing a `SEQUENCE_NUMBER` run in parallel; different numbers run in
order. A flagged row is audit information, not a failure: a rule that flags every row it
checks is still a `SUCCESS` task run.

## `HANDLER = 'EMAIL_ALERT'`

A **pipeline-level** completion alert. One email per run, whose flavour is computed from
every task's status: `SUCCESS`, `COMPLETED_WITH_ERRORS`, or `FAILED`.

| Parameter | Required | Meaning |
|---|---|---|
| `EMAIL_TO` | yes | Pipe-separated recipients. |
| `EMAIL_SUBJECT` / `EMAIL_BODY` | yes* | The default templates. Substitution tokens allowed. |
| `EMAIL_SUBJECT_<FLAVOUR>` / `EMAIL_BODY_<FLAVOUR>` | no | Per-flavour overrides, falling back to the above. |
| `EMAIL_ON_STATUS` | no | Pipe-separated flavours to send on. Absent means always. |
| `EMAIL_PIPELINES` | no | `ALL`, one code, or several — appends a status digest for those pipelines. |

\* `EMAIL_BODY` is optional when `EMAIL_PIPELINES` is set.

Substitution tokens: `$$status`, `$$pipeline_id`, `$$pipeline_code`, `$$task_code`,
`$$error_message`.
