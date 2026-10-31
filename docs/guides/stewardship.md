# Stewardship uploads

A stewardable pipeline accepts business corrections through files. Developers own its metadata
and scripts; stewards supply data using their existing accounts and pipeline permissions.

Set `STEWARDABLE_FLAG=Y` in `config/pipelines.csv`. Existing pipelines default to `N`.
Grant `CAN_UPLOAD` or higher on the pipeline using the normal permission delegation rules.
`CAN_UPLOAD` sits between `CAN_VIEW` and `CAN_RUN`; on other pipelines it permits viewing only.
There is no steward deployment role. Deployment roles other than admin do not imply a grant.

Configure one `FILE_VALIDATION` task as the only root, with a SUCCESS dependency path to every
other task. Remove its schedule. `validate` and refresh check the graph and script before use.
Marks, standing pauses and item marks cannot turn validation into SUCCESS or SKIPPED. Standalone
execution and starts without an upload are refused. Retry or rerun the original upload run to
reuse its stored files.

```yaml
Uploads:
  Max_file_mb: 50
```

This blanket limit (`uploads.max_file_mb`) is checked while bytes stream to disk. The validation
task can lower it with `MAX_FILE_MB`. Its other parameters are `SCRIPT_FILE`, `FILE_FORMAT`
(`CSV`, `TXT`, `XLSX`), optional `ENCODING` (UTF-8), `SHEET_NAME` (first sheet) and JSON
`INPUT_PARAMS`. Text always uses pipes and RFC 4180 quoting. XLSX needs `pip install
'etl-craft[excel]'`; the optional openpyxl dependency is bounded to `>=3.1.5,<4`. Legacy Excel,
macros, excessive ZIP expansion and XML DTDs are refused before the team script runs.

Put the script under `validation_scripts/`:

```python
from etl_craft.scripting import PermanentError, ValidationTask


def validate(task: ValidationTask) -> None:
    rows = task.reader.rows()
    header = next(rows, None)
    if header != ("id", "name"):
        raise PermanentError(f"row 1 header={header!r}; expected id|name; upload a corrected file")
    for number, row in enumerate(rows, 2):
        if len(row) != 2 or not str(row[0]).isdigit():
            raise PermanentError(
                f"row {number} value={row!r}; expected integer id and name; upload a corrected file"
            )
```

`ValidationTask` exposes `upload`, `reader`, `task_params`, `input_params` and `logger`, with no
warehouse connection. Returning None accepts a file; raising refuses it with the recorded
reason. Returning a value is a script defect. The reader streams text or saved workbook values;
a formula without a cached result fails naming its sheet and cell.

```console
etl-craft upload --pipeline-code CORRECTIONS corrections.csv
etl-craft uploads --pipeline-code CORRECTIONS
```

Both commands support `--api`. HTTP clients submit one multipart field named `file` to
`POST /api/v1/uploads?pipeline_code=CORRECTIONS`; GET on the same path lists receipts.
`GET /api/v1/uploads/pipelines` lists active pipelines the caller may upload to. Project-scoped
forms live under `/api/v1/projects/{project}/uploads` too. The API records `AUD_UPLOADS` with
uploader, digest, byte count, timestamps, run, outcome and per-file reason. Duplicate queued or
loaded contents are refused. Client filenames are labels, never storage paths.

The first upload creates an exclusive run. Files arriving during it wait durably; the response
names `waiting_behind_run_id`, with `pipeline_run_id` empty until admission. After that run ends,
the controller takes all pending files into one run in upload-id order, including after restart.
Validation checks each file separately and downstream tasks see only accepted files. If none
passes, the run fails before loading. Original bytes remain under `uploads/{id}/content`;
retention, archival and archive restoration belong to M2.

Python loading scripts read `task.uploads`, retaining every row's `UPLOAD_ID`. Managed SQL writes
must SELECT that column explicitly; `$$upload_ids` expands to bound ids of this run's accepted
files. Set `MERGE_DEDUPE_ORDER=UPLOAD_ID ASC` on stewardship merges. The engine binds the account
for each staged row's upload, refuses missing or foreign ids before mutation, and stamps
`CREATED_BY`/`UPDATED_BY` from that mapping. Loading scripts must also stamp their own landing
rows with each upload's account. Later uploads win for overlapping keys.

Stewarded SCD1/SCD2 uploads are supported only on native PostgreSQL and DuckDB, where S5.O.3
human-write protection is supported. Each upload's merge and protection evidence run in order
inside the same transaction. Validate and refresh refuse protected SCD upload targets on
DuckDB/Iceberg and Trino; use a supported warehouse or a non-protected ordinary target.
Ordinary upload flows and per-row SQL attribution are supported on all four local warehouses.

API-only agents fetch bytes through `GET /api/v1/agent/uploads/{upload_id}/content`, passing
`version` and `attempt_id`. Only the owning attempt token with a live lease can read its run's
files. Validation can read every file; downstream attempts cannot read refused files. Local
execution shares the same fenced read function. Both runners stream into a private cache and
verify size and SHA-256 before any script sees the file. No host path or storage credential is
sent to an agent.
