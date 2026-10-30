// The command console: send one line, print what the CLI would print. Output is text only.
const panel = document.querySelector(".console[data-run-url]");
const csrf = () => document.querySelector('meta[name="csrf-token"]')?.content || "";
const output = document.querySelector("#console-output");
const input = document.querySelector("#console-input");
const completions = panel ? JSON.parse(panel.dataset.completions || "{}") : {};
const recalled = [];
let position = 0;

function print(text, kind) {
  const part = document.createElement("span");
  if (kind) part.className = kind;
  part.textContent = text.endsWith("\n") || !text ? text : text + "\n";
  output.append(part);
  output.scrollTop = output.scrollHeight;
}

async function run(line) {
  print(`$ etl-craft ${line}`, "command");
  recalled.push(line);
  position = recalled.length;
  input.disabled = true;
  try {
    const response = await fetch(panel.dataset.runUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json", "X-CSRF-Token": csrf() },
      body: JSON.stringify({ line }),
    });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) {
      print(result.detail || result.message || `Request refused (HTTP ${response.status}).`, "error");
      return;
    }
    if (result.output) print(result.output);
    if (result.error) print(result.error, "error");
    if (result.exit_code) print(`exit ${result.exit_code}`, "note");
    loadHistory();
  } finally {
    input.disabled = false;
    input.focus();
  }
}

function complete() {
  const words = input.value.split(/\s+/);
  const last = words[words.length - 1];
  const options = words.length === 1 ? Object.keys(completions) : completions[words[0]] || [];
  const matches = options.filter((option) => option.startsWith(last)).sort();
  if (matches.length === 1) {
    words[words.length - 1] = matches[0];
    input.value = words.join(" ") + " ";
  } else if (matches.length > 1) {
    print(matches.join("  "), "note");
  }
}

async function loadHistory() {
  const list = document.querySelector("#console-history");
  const response = await fetch(panel.dataset.historyUrl, { headers: { Accept: "application/json" } });
  const audit = response.ok ? await response.json() : { actions: [] };
  list.replaceChildren();
  const recent = (audit.actions || []).slice(-15).reverse();
  for (const action of recent) {
    const item = document.createElement("li");
    item.textContent = `${action.at} · ${action.command} · ${action.outcome}`;
    list.append(item);
  }
  if (!recent.length) {
    const item = document.createElement("li");
    item.className = "note";
    item.textContent = response.ok ? "No requests yet." : "Your requests could not be read.";
    list.append(item);
  }
}

if (panel) {
  document.querySelector("#console-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const line = input.value.trim();
    input.value = "";
    if (line) run(line);
  });
  input.addEventListener("keydown", (event) => {
    if (event.key === "Tab" && input.value) {
      event.preventDefault();
      complete();
    } else if (event.key === "ArrowUp" && position > 0) {
      event.preventDefault();
      input.value = recalled[--position];
    } else if (event.key === "ArrowDown") {
      event.preventDefault();
      position = Math.min(recalled.length, position + 1);
      input.value = recalled[position] || "";
    }
  });
  loadHistory();
}
