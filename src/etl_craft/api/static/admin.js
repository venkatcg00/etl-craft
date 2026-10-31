// Administration and maintenance forms: send the filled fields as the same JSON the CLI sends and
// show the outcome; a created token is shown once. Empty fields are left out, so "change" keeps
// values. A button may preview (`data-dry-run`, or `data-method="get"` for a read), ask for
// confirmation first (`data-confirm`), and a form may show the returned document (`data-show-result`).
const csrf = () => document.querySelector('meta[name="csrf-token"]')?.content || "";
const result = document.querySelector("#admin-result");

function body(form) {
  const values = {};
  for (const field of form.querySelectorAll("input, select, textarea")) {
    if (!field.name || field.value === "") continue;
    if (field.type === "number") values[field.name] = Number(field.value);
    else if (field.type === "datetime-local") values[field.name] = new Date(field.value).toISOString();
    else values[field.name] = field.value;
  }
  return values;
}

function show(kind, text, token, returned) {
  const notice = document.createElement("section");
  notice.className = `notice ${kind}`;
  notice.setAttribute("role", kind === "error" ? "alert" : "status");
  const message = document.createElement("p");
  message.textContent = text;
  notice.append(message);
  if (token) {
    const value = document.createElement("code");
    value.id = "created-token";
    value.textContent = token;
    const copy = document.createElement("button");
    copy.type = "button";
    copy.dataset.copy = "created-token";
    copy.textContent = "Copy token";
    notice.append(value, copy);
  }
  if (returned !== undefined) {
    const detail = document.createElement("pre");
    detail.className = "result-document";
    detail.textContent = JSON.stringify(returned, null, 2);
    notice.append(detail);
  }
  const reload = document.createElement("a");
  reload.href = location.pathname + location.search;
  reload.textContent = "Reload to see the change";
  notice.append(reload);
  result.replaceChildren(notice);
  notice.scrollIntoView({ block: "nearest" });
}

document.addEventListener("submit", async (event) => {
  const form = event.target.closest("[data-api-form]");
  if (!form) return;
  event.preventDefault();
  const button = event.submitter || {};
  const url = button.dataset?.url || form.dataset.url;
  if (button.dataset?.confirm && !window.confirm(button.dataset.confirm)) return;
  const values = body(form);
  if (button.dataset && "dryRun" in button.dataset) values.dry_run = true;
  const read = button.dataset?.method === "get";
  const buttons = [...form.querySelectorAll("button:not([disabled])")];
  buttons.forEach((each) => { each.disabled = true; });
  try {
    const response = read
      ? await fetch(`${url}?${new URLSearchParams(values)}`, { headers: { Accept: "application/json" } })
      : await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json", "X-CSRF-Token": csrf() },
        body: JSON.stringify(values),
      });
    const answer = await response.json().catch(() => ({}));
    if (!response.ok) {
      show("error", answer.detail || answer.message || `Request refused (HTTP ${response.status}).`);
      return;
    }
    const shown = "showResult" in form.dataset ? answer : undefined;
    show("done", read || values.dry_run ? "Preview only: nothing changed." : "Request recorded.", answer.token, shown);
    if (!answer.token && !read && !values.dry_run) form.reset();
  } finally {
    buttons.forEach((each) => { each.disabled = false; });
  }
});
