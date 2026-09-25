"""Email alerts and SLA emails, sent through the local Mailpit relay and read back from it."""

import json
import socket
import sys
import urllib.parse
import urllib.request
import uuid
from dataclasses import replace

import pytest

from etl_craft.config import EmailConfig, EmailProfile, ExecutionLimits
from etl_craft.core.errors import HandlerError
from etl_craft.engine import runlog
from etl_craft.execution.connections import probe_email_relay
from etl_craft.execution.context import build_task_context
from etl_craft.execution.pipeline import SlaLapse, default_hooks
from etl_craft.handlers import email_alert
from etl_craft.handlers.mail import send_sla_lapse_email
from fixtures.metadata import add_dependency, add_pipeline, add_task, start_run, task_run
from fixtures.services import require


def relay_config(config, host, port):
    profile = EmailProfile(
        "EMAIL", "dev", host, port, "etl@example.com", use_tls=False, from_name="ETL Craft"
    )
    return replace(config, email=EmailConfig("dev", {"dev": profile}))


@pytest.fixture
def mailpit(engine_db):
    smtp = require("mailpit_smtp")
    api = require("mailpit_api")

    def read(subject):
        query = urllib.parse.urlencode({"query": f'subject:"{subject}"'})
        with urllib.request.urlopen(f"{api.http_url}/api/v1/search?{query}", timeout=10) as reply:
            found = json.load(reply)["messages"]
        assert len(found) == 1, found
        with urllib.request.urlopen(
            f"{api.http_url}/api/v1/message/{found[0]['ID']}", timeout=10
        ) as reply:
            return json.load(reply)

    return relay_config(engine_db.config, smtp.host, smtp.port), read


@pytest.fixture
def pipeline(engine_db):
    """Pipeline P: load succeeded, check failed, and alert watches check through FAILURE."""
    engine = engine_db.engine
    ids = {}
    with engine.begin() as conn:
        ids["P"] = add_pipeline(conn, "P")
        ids["load"] = add_task(conn, ids["P"], "load")
        ids["check"] = add_task(conn, ids["P"], "check")
        ids["alert"] = add_task(conn, ids["P"], "alert", "EMAIL_ALERT")
        add_dependency(conn, ids["P"], ids["alert"], ids["check"], "FAILURE")
        ids["run"] = start_run(conn, ids["P"])
        task_run(conn, ids["load"], ids["run"], "SUCCESS")
        failed = task_run(conn, ids["check"], ids["run"], "IN-PROGRESS")
        runlog.finish_task_run(conn, failed, status="FAILED", error_message="3 rows had no key")
        ids["alert_run"] = task_run(conn, ids["alert"], ids["run"], "IN-PROGRESS")
    return engine, ids


def alert(engine, config, ids, **params):
    context = build_task_context(engine, config, ids["alert_run"], force=False)
    return email_alert.run(replace(context, config=config, task_params=params), engine)


def test_an_alert_reports_the_run_with_a_digest(mailpit, pipeline):
    config, read = mailpit
    engine, ids = pipeline
    tag = uuid.uuid4().hex[:8]
    result = alert(
        engine,
        config,
        ids,
        EMAIL_TO="ops@example.com|lead@example.com",
        EMAIL_SUBJECT=f"{tag} $$pipeline_code run $$pipeline_id: $$status",
        EMAIL_BODY="Failures: $$error_message",
        EMAIL_PIPELINES="P|NOPE",
    )
    subject = f"{tag} P run {ids['run']}: FAILED"
    assert result.variables["EMAIL_SUBJECT"] == subject
    message = read(subject)
    assert [to["Address"] for to in message["To"]] == ["ops@example.com", "lead@example.com"]
    assert (message["From"]["Name"], message["From"]["Address"]) == ("ETL Craft", "etl@example.com")
    body = message["HTML"]
    assert "P: FAILED" in body and "Failures: 3 rows had no key" in body
    # The digest: P's latest run with its tasks, and a row for the unknown code.
    assert "<summary>P: " in body and "3 rows had no key</td>" in body
    assert "NOPE</td>" in body and "UNKNOWN" in body


def test_an_alert_on_other_outcomes_sends_nothing(mailpit, pipeline):
    config, _ = mailpit
    engine, ids = pipeline
    result = alert(
        engine,
        config,
        ids,
        EMAIL_TO="a@example.com",
        EMAIL_SUBJECT="x",
        EMAIL_BODY="y",
        EMAIL_ON_STATUS="SUCCESS",
    )
    assert result.variables == {
        "RUN_STATUS": "FAILED",
        "EMAIL_SENT": "false",
        "REASON": "FAILED is not in EMAIL_ON_STATUS",
    }


@pytest.mark.parametrize(
    ("params", "message"),
    [
        ({"EMAIL_SUBJECT": "x", "EMAIL_BODY": "y"}, "EMAIL_TO is required"),
        ({"EMAIL_TO": "a@example.com", "EMAIL_BODY": "y"}, "EMAIL_SUBJECT .* is required"),
        ({"EMAIL_TO": "a@example.com", "EMAIL_SUBJECT": "x"}, "EMAIL_BODY .* is required"),
        (
            {"EMAIL_TO": "a@example.com", "EMAIL_SUBJECT": "$$when", "EMAIL_BODY": "y"},
            r"EMAIL_SUBJECT uses unknown token\(s\) \$\$when",
        ),
        (
            {
                "EMAIL_TO": "a@example.com",
                "EMAIL_SUBJECT": "x",
                "EMAIL_BODY": "y",
                "EMAIL_ON_STATUS": "FAILED|OK",
            },
            "EMAIL_ON_STATUS names unknown outcome",
        ),
    ],
)
def test_alert_mistakes_fail_with_the_remedy(mailpit, pipeline, params, message):
    config, _ = mailpit
    engine, ids = pipeline
    with pytest.raises(HandlerError, match=message):
        alert(engine, config, ids, **params)


def test_a_relay_that_cannot_be_reached_is_named(engine_db, pipeline):
    engine, ids = pipeline
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = relay_config(engine_db.config, "127.0.0.1", port)
    with pytest.raises(HandlerError, match=rf"the relay 127\.0\.0\.1:{port} failed at connect"):
        alert(engine, config, ids, EMAIL_TO="a@example.com", EMAIL_SUBJECT="x", EMAIL_BODY="y")


def test_the_sla_email_goes_to_the_pipeline_recipients_only_when_enforced(mailpit, engine_db):
    config, read = mailpit
    tag = uuid.uuid4().hex[:8]
    code = f"SLA_{tag}".upper()
    with engine_db.engine.begin() as conn:
        pipeline_id = add_pipeline(conn, code, sla_in_hours=1)
        conn.exec_driver_sql(
            "UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS = "
            '\'{"EMAIL_RECIPIENTS": ["owner@example.com"]}\' '
            f"WHERE PIPELINE_ID = {pipeline_id}"
        )
    lapse = SlaLapse(code, 97, 1.0, 1.5)
    # Enforce_sla off: no hook, and a direct call sends nothing.
    assert default_hooks(config, engine_db.engine).on_sla_lapse is None
    send_sla_lapse_email(
        config, None, pipeline_code=code, pipeline_run_id=1, sla_hours=1, elapsed_hours=2
    )

    enforced = replace(config, limits=ExecutionLimits(enforce_sla=True))
    default_hooks(enforced, engine_db.engine).on_sla_lapse(lapse)
    message = read(f"[etl-craft] {code}: SLA of 1 h missed")
    assert [to["Address"] for to in message["To"]] == ["owner@example.com"]
    assert "pipeline_run_id=97 has been running 1.50 h, past its SLA of 1 h." in message["HTML"]
    with pytest.raises(HandlerError, match="no one to send the SLA email to"):
        send_sla_lapse_email(
            enforced, None, pipeline_code=code, pipeline_run_id=1, sla_hours=1, elapsed_hours=2
        )


FAKE_SENDMAIL = """#!{python}
import sys
open({out!r}, "w").write(" ".join(sys.argv[1:]) + "\\n" + sys.stdin.read())
sys.exit({code})
"""


def sendmail_config(config, tmp_path, code=0):
    program = tmp_path / "sendmail"
    out = tmp_path / "sent.eml"
    program.write_text(FAKE_SENDMAIL.format(python=sys.executable, out=str(out), code=code))
    program.chmod(0o755)
    profile = EmailProfile(
        "EMAIL",
        "dev",
        "",
        0,
        "etl@example.com",
        transport="sendmail",
        sendmail_path=str(program),
        from_name="ETL Craft",
    )
    return replace(config, email=EmailConfig("dev", {"dev": profile})), out


def test_an_alert_through_sendmail(engine_db, pipeline, tmp_path):
    engine, ids = pipeline
    config, out = sendmail_config(engine_db.config, tmp_path)
    alert(engine, config, ids, EMAIL_TO="ops@example.com", EMAIL_SUBJECT="$$status", EMAIL_BODY="y")
    sent = out.read_text()
    # Recipients come from the headers (-t); the sender is the profile's from_address (-f).
    assert sent.startswith("-t -oi -f etl@example.com -F ETL Craft\n")
    assert "From: ETL Craft <etl@example.com>" in sent
    assert "To: ops@example.com" in sent and "Subject: FAILED" in sent
    assert probe_email_relay(config) is None


def test_a_failing_or_missing_sendmail_is_named(engine_db, pipeline, tmp_path):
    engine, ids = pipeline
    config, _ = sendmail_config(engine_db.config, tmp_path, code=75)
    params = {"EMAIL_TO": "a@example.com", "EMAIL_SUBJECT": "x", "EMAIL_BODY": "y"}
    with pytest.raises(HandlerError, match=r"sendmail exited 75"):
        alert(engine, config, ids, **params)
    missing = replace(config.email.active, sendmail_path=str(tmp_path / "no_such_sendmail"))
    gone = replace(config, email=EmailConfig("dev", {"dev": missing}))
    with pytest.raises(HandlerError, match=r"the sendmail program .* does not exist"):
        alert(engine, gone, ids, **params)
    assert "does not exist; install one, or set sendmail_path" in probe_email_relay(gone)
