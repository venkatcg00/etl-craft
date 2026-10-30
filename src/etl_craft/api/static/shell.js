const csrf = () => document.querySelector('meta[name="csrf-token"]')?.content || "";
const destination = () => location.pathname + location.search + location.hash;
const channel = typeof BroadcastChannel === "function" ? new BroadcastChannel("etl-craft-session") : null;
const signin = () => "/signin?return_to=" + encodeURIComponent(destination());
document.addEventListener("etl-craft:session",event=>channel?.postMessage(event.detail));
channel?.addEventListener("message", event => {
  if (event.data === "signed-out") location.assign(signin());
  if (event.data === "signed-in" && !location.pathname.startsWith("/signin")) location.reload();
});
if (window.htmx) installHtmx(); else document.addEventListener("DOMContentLoaded", installHtmx, {once:true});
function installHtmx() {
  htmx.defineExtension("json-enc", {
    onEvent(name, event) { if (name === "htmx:configRequest") event.detail.headers["Content-Type"] = "application/json"; },
    encodeParameters(xhr, parameters) { xhr.overrideMimeType("text/json"); return JSON.stringify(parameters); }
  });
  document.body.addEventListener("htmx:configRequest", event => { event.detail.headers["X-CSRF-Token"] = csrf(); });
  document.body.addEventListener("htmx:beforeSwap", event => {
    if (event.detail.xhr.status >= 400) { event.detail.shouldSwap = true; event.detail.isError = false; }
  });
  document.body.addEventListener("htmx:afterRequest", event => {
    const state = event.detail.xhr.getResponseHeader("X-ETL-Session");
    if (state) document.dispatchEvent(new CustomEvent("etl-craft:session",{detail:state}));
    const selected=event.detail.requestConfig?.parameters?.theme;
    if(event.detail.successful && selected) document.dispatchEvent(new CustomEvent("etl-craft:apply-theme",{detail:{theme:selected}}));
  });
}
const shortcuts = document.querySelector("#shortcuts");
let trigger;
function openShortcuts(source) { trigger=source; shortcuts.showModal(); }
document.querySelectorAll("[data-shortcuts]").forEach(el => el.addEventListener("click", () => openShortcuts(el)));
shortcuts?.addEventListener("close", () => trigger?.focus());
document.addEventListener("keydown", event => {
  if (event.key === "Escape") document.querySelectorAll("details[open]").forEach(el => {el.open=false;el.querySelector("summary").focus();});
  if (event.key === "?" && !event.ctrlKey && !event.metaKey && !event.altKey && !event.target.closest("input,textarea,select,[contenteditable]")) { event.preventDefault();openShortcuts(document.activeElement); }
});
document.querySelectorAll("[data-copy]").forEach(button => button.addEventListener("click", async () => {
  const source=document.getElementById(button.dataset.copy);
  try { await navigator.clipboard.writeText(source.value || source.textContent);button.textContent="Copied"; }
  catch (_) { source.focus();if(source.select)source.select();button.textContent="Select and copy the details"; }
}));
const warning=document.querySelector("#session-warning");
const expiry=Date.parse(document.body.dataset.expires || "");
if (Number.isFinite(expiry)) {
  const again=document.querySelector("#signin-again");again.href=signin();
  const warn=()=>{warning.hidden=false;};
  const delay=Math.max(0,expiry-Date.now()-120000);
  atTime(warn, Date.now()+delay);
  atTime(()=>location.assign(signin()), expiry);
}
document.addEventListener("etl-craft:theme", async event => {
  const frame=document.querySelector("#catalog-frame");frame?.contentWindow?.postMessage({type:"etl-craft:set-theme",theme:event.detail.theme},location.origin);
  if (!document.querySelector(".profile")) return;
  const response=await fetch("/api/v1/account/settings", {method:"POST",headers:{"Content-Type":"application/json","X-CSRF-Token":csrf()},body:JSON.stringify({theme:event.detail.theme})});
  if(!response.ok){document.querySelector("#result").textContent="Theme could not be saved. Reload and try again.";}
});
window.addEventListener("message", event=>{
  const frame=document.querySelector("#catalog-frame");
  if(event.origin===location.origin && event.source===frame?.contentWindow && event.data?.type==="etl-craft:theme") {
    document.dispatchEvent(new CustomEvent("etl-craft:apply-theme",{detail:{theme:event.data.theme}}));
    document.dispatchEvent(new CustomEvent("etl-craft:theme",{detail:{theme:event.data.theme}}));
  }
});
document.querySelector("#catalog-frame")?.addEventListener("load",event=>event.target.contentWindow.postMessage({type:"etl-craft:set-theme",theme:document.documentElement.dataset.theme},location.origin));
if(document.body.dataset.view==="signin") import("./signin.js");

function atTime(callback, when) {
  const delay=Math.max(0,when-Date.now());
  if(delay>2147483647) setTimeout(()=>atTime(callback,when),2147483647);
  else setTimeout(callback,delay);
}
document.body.addEventListener("htmx:afterSwap",event=>{
  const error=document.querySelector("#form-error");
  if(error){
    event.detail.requestConfig?.elt?.querySelectorAll("input:not([type=hidden]),select").forEach(input=>{input.setAttribute("aria-describedby","form-error");input.setAttribute("aria-invalid","true");});
    error.focus();
  } else {
    document.querySelectorAll('[aria-describedby="form-error"]').forEach(input=>{input.removeAttribute("aria-describedby");input.removeAttribute("aria-invalid");});
  }
});
