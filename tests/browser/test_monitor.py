"""The Workflow Monitor in a real browser: select through the views, then act on the selection."""

import pytest
from playwright.sync_api import expect, sync_playwright
from sqlalchemy import text

from etl_craft.core.actor import Actor, ActorKind, acting_as
from fixtures.metadata import add_dependency, add_pipeline, add_task, finish_run, start_run

pytestmark = pytest.mark.browser
PASSWORD = "correct horse battery"


@pytest.fixture
def run_site(site):
    ctx, url = site
    with acting_as(Actor("owner", ActorKind.USER)), ctx.engine.begin() as conn:
        pipeline = add_pipeline(conn, "OPS")
        extract = add_task(conn, pipeline, "EXTRACT")
        load = add_task(conn, pipeline, "LOAD")
        add_dependency(conn, pipeline, load, extract)
        finish_run(conn, start_run(conn, pipeline))
        active = start_run(conn, pipeline)
        add_pipeline(conn, "SALES")
    return ctx, url, active


@pytest.mark.parametrize("browser_name", ["chromium", "firefox"])
def test_select_a_task_in_the_graph_and_hold_it_from_the_dialog(run_site, browser_name):
    ctx, url, active = run_site
    with sync_playwright() as pw:
        browser = getattr(pw, browser_name).launch()
        page = browser.new_context(ignore_https_errors=True).new_page()
        page.goto(url + "/signin?return_to=%2Fui%2Fprojects%2Fdefault%2Fpipelines")
        page.get_by_label("Account name", exact=True).fill("owner")
        page.get_by_label("Password", exact=True).fill(PASSWORD)
        page.get_by_role("button", name="Sign in", exact=True).click()
        page.get_by_role("link", name="OPS", exact=True).click()
        page.get_by_role("link", name=str(active), exact=True).click()
        expect(page.get_by_text(f"Selected: the whole run {active}")).to_be_visible()
        page.get_by_role("link", name="LOAD, QUEUED").or_(
            page.get_by_role("link", name="LOAD, not started")
        ).first.click()
        expect(page.get_by_role("status").filter(has_text="task LOAD")).to_be_visible()
        page.wait_for_load_state("load")
        page.get_by_role("button", name="Hold", exact=True).click()
        dialog = page.get_by_role("dialog")
        expect(dialog.get_by_text(f"task LOAD in run {active}")).to_be_visible()
        dialog.get_by_label("Reason (recorded with the request)").fill("check the load")
        dialog.get_by_role("button", name="Request", exact=True).click()
        expect(page.locator("#result")).to_contain_text("HOLD")
        expect(page.get_by_role("button", name="Hold", exact=True)).to_be_focused()
        page.go_back()
        expect(page.get_by_text(f"Selected: the whole run {active}")).to_be_visible()
        browser.close()
    with ctx.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT ACTION AS action, TASK_ID AS task_id, REASON AS reason "
                "FROM AUD_CONTROL_REQUESTS WHERE PIPELINE_RUN_ID=:run"
            ),
            {"run": active},
        ).all()
    assert [(row.action, row.reason) for row in rows] == [("HOLD", "check the load")]


def test_a_refused_control_keeps_the_dialog_open_with_the_servers_reason(run_site):
    _, url, active = run_site
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_context(ignore_https_errors=True).new_page()
        page.goto(url + "/signin?return_to=%2Fui%2Fprojects%2Fdefault%2Fpipelines%2FOPS")
        page.get_by_label("Account name", exact=True).fill("owner")
        page.get_by_label("Password", exact=True).fill(PASSWORD)
        page.get_by_role("button", name="Sign in", exact=True).click()
        page.wait_for_load_state("load")
        page.get_by_role("button", name="Pause", exact=True).click()
        dialog = page.get_by_role("dialog")
        dialog.get_by_role("button", name="Request", exact=True).click()
        expect(page.locator("#control-error")).to_contain_text("reason")
        dialog.get_by_label("Reason (recorded with the request)").fill("maintenance")
        dialog.get_by_role("button", name="Request", exact=True).click()
        expect(page.locator("#result")).to_contain_text("PAUSE")
        page.goto(url + f"/ui/projects/default/pipelines/OPS/runs/{active}?task=LOAD")
        page.evaluate("document.querySelector('#control-dialog').dataset.run = '999999'")
        page.wait_for_load_state("load")
        page.get_by_role("button", name="Hold", exact=True).click()
        dialog.get_by_label("Reason (recorded with the request)").fill("stale page")
        dialog.get_by_role("button", name="Request", exact=True).click()
        error = page.locator("#control-error")
        expect(error).to_be_visible()
        expect(page.get_by_role("dialog")).to_be_visible()
        browser.close()


def sign_in_to(page, url, destination):
    page.goto(url + "/signin?return_to=" + destination.replace("/", "%2F"))
    page.get_by_label("Account name", exact=True).fill("owner")
    page.get_by_label("Password", exact=True).fill(PASSWORD)
    page.get_by_role("button", name="Sign in", exact=True).click()


def test_bulk_pause_run_now_and_csv_export(run_site, tmp_path):
    ctx, url, _ = run_site
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_context(ignore_https_errors=True, accept_downloads=True).new_page()
        sign_in_to(page, url, "/ui/projects/default/pipelines/SALES")
        dialog = page.get_by_role("dialog")
        page.wait_for_load_state("load")
        page.get_by_role("button", name="Run now", exact=True).click()
        dialog.get_by_label("Reason (recorded with the request)").fill("manual check")
        dialog.get_by_role("button", name="Request", exact=True).click()
        expect(page.locator("#result")).to_contain_text("SALES: pipeline_run_id=")
        page.goto(url + "/ui/projects/default/pipelines")
        page.wait_for_load_state("load")
        pause = page.get_by_role("button", name="Pause selected")
        expect(pause).to_be_disabled()
        page.get_by_label("Select OPS").check()
        page.get_by_label("Select SALES").check()
        expect(page.get_by_text("2 pipelines selected")).to_be_visible()
        page.wait_for_load_state("load")
        pause.click()
        expect(dialog.get_by_text("2 selected pipelines: OPS, SALES")).to_be_visible()
        dialog.get_by_label("Reason (recorded with the request)").fill("maintenance")
        dialog.get_by_role("button", name="Request", exact=True).click()
        expect(page.locator("#result")).to_contain_text("OPS: requested")
        expect(page.locator("#result")).to_contain_text("SALES: requested")
        with page.expect_download() as download:
            page.get_by_role("button", name="Export CSV").click()
        saved = tmp_path / "pipelines.csv"
        download.value.save_as(saved)
        lines = saved.read_text().splitlines()
        assert lines[0].startswith("Pipeline,Name,Tags,Schedule,Next run")
        assert {line.split(",")[0] for line in lines[1:]} == {"OPS", "SALES"}
        browser.close()
    with ctx.engine.connect() as conn:
        paused = set(
            conn.execute(
                text(
                    "SELECT p.PIPELINE_CODE FROM CFG_PIPELINES p WHERE EXISTS (SELECT 1 FROM "
                    "AUD_PIPELINE_PAUSES a WHERE a.PIPELINE_ID=p.PIPELINE_ID "
                    "AND a.TASK_ID IS NULL AND a.RESUMED_AT IS NULL)"
                )
            ).scalars()
        )
        runs = conn.execute(
            text(
                "SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG r JOIN CFG_PIPELINES p "
                "ON p.PIPELINE_ID=r.PIPELINE_ID WHERE p.PIPELINE_CODE='SALES'"
            )
        ).scalar_one()
    assert paused == {"OPS", "SALES"} and runs == 1


@pytest.mark.parametrize("browser_name", ["chromium", "firefox"])
def test_search_shortcut_finds_a_run_and_remembers_recently_viewed_pages(run_site, browser_name):
    _, url, active = run_site
    with sync_playwright() as pw:
        browser = getattr(pw, browser_name).launch()
        page = browser.new_context(ignore_https_errors=True).new_page()
        sign_in_to(page, url, "/ui/projects/default/pipelines/OPS")
        expect(page.get_by_role("heading", name="OPS")).to_be_visible()
        page.wait_for_load_state("load")
        page.locator("main").click()
        page.keyboard.press("/")
        expect(page.get_by_role("searchbox", name="Search")).to_be_focused()
        page.keyboard.type(str(active))
        page.keyboard.press("Enter")
        page.get_by_role("link", name=f"Run {active}", exact=True).click()
        expect(page.get_by_text(f"Selected: the whole run {active}")).to_be_visible()
        page.wait_for_load_state("load")
        page.goto(url + "/ui/projects/default/search")
        recent = page.locator("#recent-list a")
        expect(recent).to_have_text([f"Run {active} · OPS", "Pipeline OPS"])
        browser.close()


def test_live_updates_refresh_the_page_or_warn_while_a_dialog_is_open(run_site, monkeypatch):
    from etl_craft.api import ui_live

    monkeypatch.setattr(ui_live, "CHECK_SECONDS", 1)
    ctx, url, active = run_site
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_context(ignore_https_errors=True).new_page()
        sign_in_to(page, url, "/ui/projects/default/pipelines/OPS")
        page.wait_for_load_state("load")
        expect(page.locator("#live-updated")).to_contain_text("s ago")
        with acting_as(Actor("owner", ActorKind.USER)), ctx.engine.begin() as conn:
            finish_run(conn, active)
        live = page.locator("#live")
        expect(live.get_by_title(f"Run {active} · IN-PROGRESS")).to_have_count(0, timeout=15000)
        page.get_by_role("button", name="Pause", exact=True).click()
        with acting_as(Actor("owner", ActorKind.USER)), ctx.engine.begin() as conn:
            pipeline = conn.execute(
                text("SELECT PIPELINE_ID FROM CFG_PIPELINES WHERE PIPELINE_CODE='OPS'")
            ).scalar_one()
            newer = start_run(conn, pipeline)
        expect(page.locator("#live-notice")).to_have_text("This page has changed since it loaded.")
        newer_link = page.locator("#live").get_by_role("link", name=str(newer), exact=True)
        expect(newer_link).to_have_count(0)
        page.get_by_role("dialog").get_by_role("button", name="Cancel").click()
        expect(newer_link).to_be_visible()
        expect(page.locator("#live-notice")).to_have_text("")
        browser.close()


def test_attempt_log_follows_new_lines_until_the_attempt_ends(run_site):
    from datetime import UTC, datetime, timedelta

    from etl_craft.core.enums import AttemptStatus
    from etl_craft.engine import transitions

    ctx, url, active = run_site
    owner = Actor("owner", ActorKind.USER)
    with acting_as(owner), ctx.engine.begin() as conn:
        task = conn.execute(
            text("SELECT TASK_ID FROM CFG_TASKS WHERE TASK_CODE='EXTRACT'")
        ).scalar_one()
        summary = transitions.find_or_create_task_run(conn, task, active).task_run_id
        attempt = transitions.queue_attempt(conn, summary, owner)
        lease = datetime.now(UTC) + timedelta(minutes=5)
        transitions.claim_attempt(conn, attempt, owner, owner="w1", lease_expires_at=lease)
        transitions.start_attempt(conn, attempt, owner, owner="w1", host="worker-1")
        log = ctx.config.log_dir / "project-1" / "OPS" / f"run-{active}" / "EXTRACT.attempt-1.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text("reading orders\n")
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH=:path WHERE ATTEMPT_ID=:id"),
            {"path": str(log.absolute()), "id": attempt},
        )
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_context(ignore_https_errors=True).new_page()
        sign_in_to(page, url, f"/ui/projects/default/pipelines/OPS/runs/{active}?task=EXTRACT")
        page.get_by_role("link", name="Attempt 1 log and details").click()
        shown = page.locator("#log")
        expect(shown).to_have_text("reading orders\n")
        with log.open("a") as stream:
            stream.write("wrote 42 rows\n")
        expect(shown).to_have_text("reading orders\nwrote 42 rows\n")
        with acting_as(owner), ctx.engine.begin() as conn:
            transitions.finish_attempt(conn, attempt, AttemptStatus.SUCCESS, owner, owner="w1")
        expect(page.locator("#log-state")).to_contain_text("Attempt ended SUCCESS")
        browser.close()


def test_console_completes_runs_and_recalls_a_command(run_site):
    _, url, _ = run_site
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_context(ignore_https_errors=True).new_page()
        sign_in_to(page, url, "/ui/projects/default/console")
        page.wait_for_url("**/ui/projects/default/console")
        page.wait_for_load_state("load")
        line = page.get_by_role("textbox", name="Command")
        line.fill("pipel")
        line.press("Tab")
        expect(line).to_have_value("pipelines ")
        line.press("Enter")
        output = page.get_by_role("log")
        expect(output).to_contain_text("$ etl-craft pipelines")
        expect(output).to_contain_text("OPS")
        line.fill("controller")
        line.press("Enter")
        expect(output).to_contain_text("not a command you can run in the console")
        line.press("ArrowUp")
        expect(line).to_have_value("controller")
        browser.close()


def test_task_actions_pause_with_a_standing_outcome_and_mark_from_the_dialog(run_site):
    ctx, url, active = run_site
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_context(ignore_https_errors=True).new_page()
        run_page = f"/ui/projects/default/pipelines/OPS/runs/{active}?task=LOAD"
        sign_in_to(page, url, run_page)
        page.wait_for_url(f"**{run_page}")
        page.wait_for_load_state("load")
        page.get_by_role("button", name="Pause task", exact=True).click()
        dialog = page.get_by_role("dialog")
        expect(dialog.get_by_text("task LOAD in every run")).to_be_visible()
        dialog.get_by_label("While paused, downstream tasks treat this task as").select_option(
            "SKIPPED"
        )
        dialog.get_by_label("Reason (recorded with the request)").fill("source late")
        dialog.get_by_role("button", name="Request", exact=True).click()
        expect(page.locator("#result")).to_contain_text("Pause task")
        page.reload()
        page.wait_for_load_state("load")
        page.get_by_role("button", name="Mark as", exact=True).click()
        dialog.get_by_label("Mark as", exact=True).select_option("FAILED")
        dialog.get_by_label("Reason (recorded with the request)").fill("bad input")
        dialog.get_by_role("button", name="Request", exact=True).click()
        expect(page.locator("#result")).to_contain_text("Mark as")
        browser.close()
    with ctx.engine.connect() as conn:
        pause = conn.execute(
            text(
                "SELECT p.OUTCOME AS outcome, p.REASON AS reason FROM AUD_PIPELINE_PAUSES p "
                "JOIN CFG_TASKS t ON t.TASK_ID=p.TASK_ID WHERE t.TASK_CODE='LOAD'"
            )
        ).one()
        marked = conn.execute(
            text(
                "SELECT r.STATUS FROM AUD_TASK_RUN_LOG r JOIN CFG_TASKS t ON t.TASK_ID=r.TASK_ID "
                "WHERE t.TASK_CODE='LOAD' AND r.PIPELINE_RUN_ID=:run"
            ),
            {"run": active},
        ).scalar_one()
    assert (pause.outcome, pause.reason, marked) == ("SKIPPED", "source late", "FAILED")


def test_a_run_duration_tile_opens_its_run(run_site):
    _, url, active = run_site
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_context(ignore_https_errors=True).new_page()
        sign_in_to(page, url, "/ui/projects/default/pipelines/OPS")
        page.wait_for_url("**/ui/projects/default/pipelines/OPS")
        tile = page.locator("figure.bars a").last
        box = tile.bounding_box()
        assert box is not None and box["width"] >= 20 and box["height"] >= 24
        tile.click()
        page.wait_for_url(f"**/runs/{active}")
        expect(page.get_by_role("heading", level=1)).to_contain_text(f"Run {active}")
        browser.close()
