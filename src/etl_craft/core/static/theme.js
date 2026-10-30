/* Theme control shared by generated offline catalogs and the authenticated console. */
(function () {
  "use strict";
  const root = document.documentElement;
  const server = root.dataset.themeServer === "true";
  const choices = ["system", "light", "dark"];
  let theme = choices.includes(root.dataset.theme) ? root.dataset.theme : "system";
  if (!server && !root.dataset.theme) {
    try { theme = localStorage.getItem("etl-craft.theme") || "system"; }
    catch (_) { theme = "system"; }
  }
  function apply(value) {
    theme = choices.includes(value) ? value : "system";
    root.dataset.theme = theme;
    document.querySelectorAll("[data-theme-label]").forEach(el => { el.textContent = theme[0].toUpperCase() + theme.slice(1); });
    document.querySelectorAll("[data-theme-toggle]").forEach(el => { el.setAttribute("aria-label", "Theme: " + theme + ". Change theme for your account."); });
  }
  apply(theme);
  document.addEventListener("etl-craft:apply-theme", event => apply(event.detail.theme));
  document.querySelectorAll("[data-theme-toggle]").forEach(button => button.addEventListener("click", () => {
    apply(choices[(choices.indexOf(theme) + 1) % choices.length]);
    if (!server) {
      try { localStorage.setItem("etl-craft.theme", theme); }
      catch (_) { button.title = "Theme applies to this page only; preference storage is unavailable."; }
    }
    document.dispatchEvent(new CustomEvent("etl-craft:theme", { detail: { theme } }));
    if (parent !== window) parent.postMessage({ type: "etl-craft:theme", theme }, location.origin);
  }));
  window.addEventListener("message", event => {
    if (event.origin === location.origin && event.source === parent && event.data?.type === "etl-craft:set-theme") apply(event.data.theme);
  });
}());
