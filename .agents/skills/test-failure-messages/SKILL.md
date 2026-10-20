---
name: test-failure-messages
description: Test etl-craft's "fail rather than guess" rule - every bad metadata row, file, connection, credential, upload or data value fails before work starts, with a message naming the object, the value found, what was expected and the remedy, an exit code of its own class, and a record in the audit tables and the attempt log; nothing warns and continues. Use when a change adds or changes a validation, an error, a refusal, validate, doctor or config apply, or when auditing operability before a release.
---

# Test failure messages

A failure someone can fix at 3 a.m. from the message alone is a feature. A warning that lets a
run continue on a guess is a defect, even when the run succeeds.

## How every check is written

- **Assert the four parts of the message**, each as its own substring, not the whole string:
  1. the exact object (pipeline and task code, file path, column, profile, variable);
  2. the value found;
  3. what was expected;
  4. the remedy (the command or setting that fixes it).
- **Assert the class and exit code.** The error comes from `core/errors.py`, never `ValueError` or
  `RuntimeError`; the CLI exits with its `ExitCode`; the API answers with the matching status.
- **Assert where it is recorded.** A refusal of a request has its audit row; a failed attempt
  has the message in `AUD_` and in the attempt log.
- **Assert it failed early.** Nothing was written: no run, no attempt, no target change, no
  partial file. The earliest place that can know (`validate`, `doctor`, `config apply`, the
  request check) refuses it.
- **Prove the test bites:** replace the refusal with a warning and see the test fail.

## Checklist

### Finding warn-and-continue code

1. `git grep -n "logger.warning\|warnings.warn" src` and read each one: a warning about input
   the engine then uses is a defect. A warning about the environment that changes nothing (a
   deprecated setting with no behaviour) is fine.
2. `git grep -n "except .*:\s*$" src` near `pass`, `continue` or a default value: a swallowed
   error that falls back is a defect, unless it sits at a process boundary that records it.
3. Defaults picked when a value is missing (`or "default"`, `.get(key, ...)` on user input):
   the value must be required, or the default documented and recorded.

### Inputs that must be refused before a run

Each row: the bad input, and the command that must refuse it.

| Bad input | Refused by |
|---|---|
| Missing SQL file or script, unreadable file, wrong encoding | `validate` |
| A SQL file with no SELECT, two statements or a write | `validate`, `SqlGuardError` |
| Unknown target, schema or column; a type that cannot hold the source | `validate` |
| A dependency on an unknown or inactive task or pipeline; a cycle | `validate`, `GraphError` |
| Unknown `PIPELINE_PARAMETERS` key or task parameter; a bad Y/N flag | `validate`, `config apply` |
| A config CSV with an unknown column, a duplicate key or a missing required value | `config apply` |
| A missing environment variable named in `craft-connector.yml`; a literal secret | `doctor`, load |
| An unreachable warehouse or expired credential | `doctor`, attempt start |
| An unwritable logs or uploads directory, a full disk | attempt start (S7.P) |
| A bad stewardship file | its `FILE_VALIDATION` script (S7.M) |

1. For every row, a test sends the bad input and asserts the four message parts and that no run
   was created.
2. Generate cases where a list exists: every config file's required columns, every task
   parameter, every handler's required settings.

### Messages at runtime

1. A failed SQL statement names its step, the task and the warehouse, and leaves no scratch
   tables (`test_a_failing_statement_names_its_step_and_leaves_no_scratch_tables`).
2. A driver error is wrapped with the object it was about; the driver's text is kept, with any
   secret in it masked.
3. Every error class has a unique `ExitCode` (a test enforces it); a new class takes the next
   free code.

## Running

```bash
uv run pytest -q -p no:cacheprovider tests/integration/engine/test_validate.py \
  tests/integration/engine/test_doctor_setup.py tests/integration/engine/test_config_files.py
```

## Report

Per row: the test, the four message parts asserted, the exit code, and the audit record. A row
whose test asserts only "it failed" is a gap.
