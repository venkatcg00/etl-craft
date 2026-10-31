// Administration forms: send the filled fields as the same JSON the CLI sends, show the outcome,
// and show a newly created token once. Empty fields are left out, so "change" keeps values.
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

function show(kind, text, token) {
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
  const url = event.submitter?.dataset.url || form.dataset.url;
  const buttons = form.querySelectorAll("button");
  buttons.forEach((button) => { button.disabled = true; });
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json", "X-CSRF-Token": csrf() },
      body: JSON.stringify(body(form)),
    });
    const answer = await response.json().catch(() => ({}));
    if (!response.ok) {
      show("error", answer.detail || answer.message || `Request refused (HTTP ${response.status}).`);
      return;
    }
    show("done", "Request recorded.", answer.token);
    if (!answer.token) form.reset();
  } finally {
    buttons.forEach((button) => { button.disabled = false; });
  }
});
