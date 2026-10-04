# Email alerts

A pipeline reports how a run went with one email, sent by a task with
`HANDLER = 'EMAIL_ALERT'` at the end of the pipeline. With `Orchestration.Enforce_sla: true`, the
engine also emails when a run misses its pipeline's SLA.

## How email is sent

The `Email` block of the active `Orchestration` profile says how, in one of two ways:

| `transport` | Sends through | Settings |
|---|---|---|
| `smtp` (the default) | an SMTP relay | `host`, `port`, `from_address`, `from_name`, `tls_mode`, optional `ca_file`, and `auth_mode`: `none`, `password` (`user`, `secret`) or `oauth` (SMTP XOAUTH2: `user`, `client_id`, `secret`, `token_url`) |
| `sendmail` | the host's own `sendmail` program, the one `mailx` and `mail` use | `from_address`, `from_name`, and `sendmail_path` when it is not `/usr/sbin/sendmail` |

`from_address` is the sender's address and `from_name`, optional, the name shown with it, as in
`ETL Craft <etl@example.com>`. With `sendmail` the engine hands each message to the program, with the recipients taken from its
headers and `from_address` as the sender, and the host's mail system delivers it. Host, port and
login settings are refused there, since they would be ignored. Before a run starts, the engine
checks the relay answers using its TLS mode, or the program exists and can run. A failed email
preflight is logged and data tasks may still run; the alert task records any delivery failure.

`tls_mode` is `starttls` (the default), `ssl` for implicit TLS (usually port 465), or `none`
for an unauthenticated internal relay. Both TLS modes verify the relay's certificate and hostname.
`ca_file` names a CA certificate file; a relative path starts at the project directory. Omit it
to use system trust. `doctor` shows the mode and trust source and checks the TLS connection without
logging in. Password and OAuth authentication require `starttls` or `ssl`; `none` is refused with
either login mode.

The existing `use_tls` setting remains supported: `true` means `starttls`, and `false` means `none`.
Prefer `tls_mode` for new profiles; conflicting `tls_mode` and `use_tls` settings are refused.
With `sendmail`, TLS and login settings are refused because the host's mail system owns delivery.

## The alert task

An alert task depends on the tasks it reports on, usually through `ALWAYS` dependencies, so it runs
once they have finished. It works out the run's outcome from every other task's status:

| Outcome | When |
|---|---|
| `FAILED` | any task failed |
| `COMPLETED_WITH_ERRORS` | none failed, but a task was skipped, needed a retry, succeeded with an error message or has not finished, or the run is already past its SLA with `Enforce_sla` on |
| `SUCCESS` | every task succeeded cleanly |

Other alert tasks are left out, so two alerts (one to operations on `FAILED`, one to stakeholders
on `SUCCESS`) agree.

| Parameter | Value |
|---|---|
| `EMAIL_TO` | the recipients, separated by `\|`; required |
| `EMAIL_SUBJECT` | the subject; required |
| `EMAIL_BODY` | an opening paragraph; required unless `EMAIL_PIPELINES` is set |
| `EMAIL_SUBJECT_<OUTCOME>`, `EMAIL_BODY_<OUTCOME>` | the subject or body for one outcome, such as `EMAIL_SUBJECT_FAILED` |
| `EMAIL_ON_STATUS` | the outcomes to send on, separated by `\|`; every outcome when unset |
| `EMAIL_PIPELINES` | `ALL`, or pipeline codes separated by `\|`: adds a table of each one's latest run, with a section per pipeline listing its tasks |

On an outcome not in `EMAIL_ON_STATUS`, the task succeeds without sending, and its task log says
why.

Subjects and bodies may use these tokens:

| Token | Becomes |
|---|---|
| `$$status` | the outcome |
| `$$pipeline_id` | the run's `pipeline_run_id` |
| `$$pipeline_code`, `$$task_code` | the pipeline's and the alert task's codes |
| `$$error_message` | the error messages of the tasks this one watches through `FAILURE` dependencies |

Any other `$$` token fails the task, naming it.
After substitution, the subject collapses whitespace to one space, removes other control
characters, and is limited to 200 characters, ending in `…` when shortened. The body keeps the
full error message, including line breaks.

## SLA emails

With `Enforce_sla: true`, a run of a pipeline with `SLA_IN_HOURS` that is still going when its SLA
passes gets one SLA email, at that moment; a run that ends late is emailed as it ends. The email
goes to the pipeline's `EMAIL_RECIPIENTS` in `PIPELINE_PARAMETERS`, else the email recipients in
the `Orchestration` DAG defaults. When neither is set, the engine logs that the SLA email could not
be sent and why; the run itself is not affected. With `Enforce_sla` off, runs are still marked `MET`
or `BREACHED`, and nothing is sent.

## When sending fails

When the relay refuses only some recipients, the task succeeds and records `EMAIL_WARNING` in
`TASK_LOG` and a WARNING in the attempt log, naming each refused address and the relay's answer.
A total refusal or another delivery error fails the alert task.

Failed alert tasks stay `FAILED` in the audit tables and logs and are named at ERROR when the
pipeline ends. They do not change a data pipeline's outcome: successful data tasks still produce
a `SUCCESS` run, failed data tasks a `FAILED` run, and all-skipped data tasks a `SKIPPED` run.
A pipeline containing only alerts uses its alert tasks' outcomes. Task dependency edges still
judge an alert's actual task status, so put alerts at the end of the data pipeline. In remote mode,
the engine's finalization follows the same rule; the external orchestrator owns its own task and
DAG failure policy.

The task fails with a message naming the relay and the step (`connect`, `start TLS`, `log in`,
`send`, or `build headers`) and the relay's own answer, or the `sendmail` program and its exit
status and output:

```
email to ops@example.com was not sent: the relay smtp.example.com:587 failed at log in: SMTPAuthenticationError: (535, b'5.7.8 Authentication failed')
```
