// Controls on the selected scope: preview the exact scope and effect, then post the same
// request the CLI sends. Receipt is not completion: the result shows the recorded status.
const dialog = document.querySelector("#control-dialog");
const form = document.querySelector("#control-form");
const csrf = () => document.querySelector('meta[name="csrf-token"]')?.content || "";
let pending = null;
let opener = null;

function field(name, value) {
  dialog.querySelector(`[data-field="${name}"]`).textContent = value;
}

function open(button) {
  opener = button;
  const action = button.dataset.control;
  pending = { action, key: crypto.randomUUID() };
  field("action", action.replaceAll("_", " ").toLowerCase());
  field("scope", button.dataset.scope);
  field("effect", button.dataset.effect);
  field("preserves", button.dataset.preserves === "yes" ? "Kept" : "Not kept: this creates new work");
  const resume = action === "RESUME" && button.dataset.scheduled === "yes";
  dialog.querySelector(".resume-only").hidden = !resume;
  dialog.querySelector("#control-error").hidden = true;
  form.reset();
  dialog.showModal();
}

function body() {
  const request = {
    pipeline_code: dialog.dataset.pipeline,
    action: pending.action,
    reason: form.reason.value,
    request_key: pending.key,
  };
  const definition = pending.action === "PAUSE" || pending.action === "RESUME";
  if (!definition && dialog.dataset.run) request.run_id = Number(dialog.dataset.run);
  if (dialog.dataset.task) request.task_code = dialog.dataset.task;
  if (pending.action === "RESUME" && !dialog.querySelector(".resume-only").hidden) request.then = form.then.value;
  return request;
}

function announce(result) {
  const region = document.querySelector("#result");
  const section = document.createElement("section");
  section.className = "notice";
  section.setAttribute("role", "status");
  const heading = document.createElement("h2");
  const state = result.status || result.outcome || "REQUESTED";
  heading.textContent = `${pending.action.replaceAll("_", " ")} ${state}`;
  const text = document.createElement("p");
  text.textContent = (result.message || "Request recorded.") + (result.control_id ? ` Control ${result.control_id}.` : "");
  const reload = document.createElement("a");
  reload.href = location.pathname + location.search;
  reload.textContent = "Reload to see the current state";
  section.append(heading, text, reload);
  region.replaceChildren(section);
}

async function submit(event) {
  event.preventDefault();
  const error = dialog.querySelector("#control-error");
  const confirm = dialog.querySelector("#control-confirm");
  confirm.disabled = true;
  try {
    const response = await fetch(dialog.dataset.url, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json", "X-CSRF-Token": csrf() },
      body: JSON.stringify(body()),
    });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) {
      error.textContent = result.detail || result.message || `Request refused (HTTP ${response.status}).`;
      error.hidden = false;
      error.focus();
      return;
    }
    dialog.close();
    announce(result);
    document.querySelector("#result").focus();
  } finally {
    confirm.disabled = false;
  }
}

if (dialog) {
  document.querySelectorAll("[data-control]").forEach((button) => button.addEventListener("click", () => open(button)));
  form.addEventListener("submit", submit);
  dialog.querySelector("#control-cancel").addEventListener("click", () => dialog.close());
  dialog.addEventListener("close", () => opener?.focus());
}
