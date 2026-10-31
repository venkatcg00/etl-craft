// Upload one file with the same multipart request `etl-craft upload` sends, then show the receipt.
const form = document.querySelector("#upload-form");
const result = document.querySelector("#upload-result");
const csrf = () => document.querySelector('meta[name="csrf-token"]')?.content || "";

function say(kind, text) {
  const notice = document.createElement("section");
  notice.className = `notice ${kind}`;
  notice.setAttribute("role", kind === "error" ? "alert" : "status");
  const line = document.createElement("p");
  line.textContent = text;
  const reload = document.createElement("a");
  reload.href = `${location.pathname}?pipeline=${encodeURIComponent(form.pipeline_code.value)}`;
  reload.textContent = "Reload to see its state";
  notice.append(line, reload);
  result.replaceChildren(notice);
}

form?.pipeline_code.addEventListener("change", () => {
  location.search = `?pipeline=${encodeURIComponent(form.pipeline_code.value)}`;
});

form?.addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = form.querySelector("button[type=submit]");
  const body = new FormData();
  body.append("file", form.file.files[0]);
  button.disabled = true;
  try {
    const response = await fetch(`${form.dataset.url}?pipeline_code=${encodeURIComponent(form.pipeline_code.value)}`, {
      method: "POST",
      headers: { Accept: "application/json", "X-CSRF-Token": csrf() },
      body,
    });
    const answer = await response.json().catch(() => ({}));
    if (!response.ok) {
      say("error", answer.message || answer.detail || `Upload refused (HTTP ${response.status}).`);
      return;
    }
    say("done", answer.pipeline_run_id
      ? `Upload ${answer.upload_id} received; run ${answer.pipeline_run_id} validates and loads it.`
      : `Upload ${answer.upload_id} received; it waits for run ${answer.waiting_behind_run_id} to end, then runs.`);
    form.file.value = "";
  } finally {
    button.disabled = false;
  }
});
