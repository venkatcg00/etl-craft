/* The etl-craft catalog: search, and the lineage graph's filters and column tracing. */
(function () {
  "use strict";

  var root = document.body.getAttribute("data-root") || "";
  var index = window.CATALOG_INDEX || [];
  var KINDS = ["pipeline", "task", "table", "column", "rule", "script"];

  function words(query) {
    return query.toLowerCase().split(/\s+/).filter(function (w) { return w; });
  }

  function subsequence(needle, haystack) {
    var at = 0;
    for (var i = 0; i < haystack.length && at < needle.length; i++) {
      if (haystack[i] === needle[at]) at++;
    }
    return at === needle.length;
  }

  // A match on the name counts most, at a word start more, an exact name most of all; then
  // the description and documentation; then the letters in order within the name.
  function score(entry, tokens) {
    var name = entry[1].toLowerCase();
    var text = entry[2].toLowerCase();
    var total = 0;
    for (var i = 0; i < tokens.length; i++) {
      var token = tokens[i];
      var at = name.indexOf(token);
      if (at >= 0) {
        total += 20;
        if (at === 0 || /[._\s\/-]/.test(name.charAt(at - 1))) total += 10;
        if (name === token) total += 30;
      } else if (text.indexOf(token) >= 0) {
        total += 5;
      } else if (subsequence(token, name)) {
        total += 2;
      } else {
        return 0;
      }
    }
    return total - name.length / 1000;
  }

  function search(query, kinds) {
    var tokens = words(query);
    if (!tokens.length) return [];
    var found = [];
    for (var i = 0; i < index.length; i++) {
      if (kinds && kinds.indexOf(index[i][0]) < 0) continue;
      var s = score(index[i], tokens);
      if (s > 0) found.push([s, index[i]]);
    }
    found.sort(function (a, b) { return b[0] - a[0]; });
    return found.map(function (pair) { return pair[1]; });
  }

  function resultLink(entry) {
    var link = document.createElement("a");
    link.className = "result";
    link.href = root + entry[3];
    var kind = document.createElement("span");
    kind.className = "kind";
    kind.textContent = entry[0];
    link.appendChild(kind);
    link.appendChild(document.createTextNode(entry[1]));
    if (entry[2]) {
      var snippet = document.createElement("span");
      snippet.className = "snippet";
      snippet.textContent = entry[2].length > 140 ? entry[2].slice(0, 139) + "…" : entry[2];
      link.appendChild(snippet);
    }
    return link;
  }

  // The search box on every page: the best matches as you type; Enter opens all of them.
  var box = document.querySelector(".search input");
  var dropdown = document.querySelector(".search .results");
  if (box && dropdown) {
    var active = -1;
    box.addEventListener("input", function () {
      dropdown.textContent = "";
      active = -1;
      search(box.value).slice(0, 8).forEach(function (entry) {
        dropdown.appendChild(resultLink(entry));
      });
    });
    box.addEventListener("keydown", function (event) {
      var links = dropdown.querySelectorAll(".result");
      if (event.key === "ArrowDown" || event.key === "ArrowUp") {
        event.preventDefault();
        if (!links.length) return;
        active = (active + (event.key === "ArrowDown" ? 1 : -1) + links.length) % links.length;
        links.forEach(function (l, i) { l.classList.toggle("active", i === active); });
      } else if (event.key === "Enter") {
        event.preventDefault();
        if (active >= 0 && links[active]) {
          window.location.href = links[active].href;
        } else {
          window.location.href = root + "index.html#q=" + encodeURIComponent(box.value);
        }
      } else if (event.key === "Escape") {
        dropdown.textContent = "";
      }
    });
    document.addEventListener("click", function (event) {
      if (!event.target.closest(".search")) dropdown.textContent = "";
    });
  }

  // The full results on the home page, filtered by kind.
  var page = document.getElementById("search-page");
  if (page) {
    var input = page.querySelector("input");
    var chips = page.querySelector(".chips");
    var list = page.querySelector(".all-results");
    var chosen = [];
    KINDS.forEach(function (kind) {
      var chip = document.createElement("button");
      chip.type = "button";
      chip.textContent = kind;
      chip.addEventListener("click", function () {
        var at = chosen.indexOf(kind);
        if (at >= 0) chosen.splice(at, 1); else chosen.push(kind);
        chip.classList.toggle("on", at < 0);
        render();
      });
      chips.appendChild(chip);
    });
    var render = function () {
      list.textContent = "";
      var results = search(input.value, chosen.length ? chosen : null);
      results.slice(0, 200).forEach(function (entry) { list.appendChild(resultLink(entry)); });
      var summary = page.querySelector(".summary");
      summary.textContent = input.value
        ? results.length + " result(s)" + (results.length > 200 ? ", the first 200 shown" : "")
        : "";
    };
    input.addEventListener("input", render);
    var fromHash = /^#q=(.*)$/.exec(window.location.hash);
    if (fromHash) {
      input.value = decodeURIComponent(fromHash[1]);
      render();
    }
  }

  // The lineage graph: filter by direction and depth, and trace a column through every path.
  document.querySelectorAll(".graph-panel").forEach(function (panel) {
    var svg = panel.querySelector("svg.lineage");
    if (!svg) return;
    var depth = panel.querySelector("select.depth");
    var direction = panel.querySelector("select.direction");
    var nodes = svg.querySelectorAll(".node");
    var edges = svg.querySelectorAll(".edge");

    function filter() {
      var limit = depth.value === "all" ? Infinity : parseInt(depth.value, 10);
      var shown = {};
      nodes.forEach(function (node) {
        var level = parseInt(node.getAttribute("data-level"), 10);
        var hide = Math.abs(level) > limit ||
          (direction.value === "upstream" && level > 0) ||
          (direction.value === "downstream" && level < 0);
        node.classList.toggle("hidden", hide);
        if (!hide) shown[node.getAttribute("data-table")] = true;
      });
      edges.forEach(function (edge) {
        var both = shown[edge.getAttribute("data-source")] && shown[edge.getAttribute("data-target")];
        edge.classList.toggle("hidden", !both);
      });
    }

    var into = {};
    var outOf = {};
    edges.forEach(function (edge) {
      var from = edge.getAttribute("data-from");
      var to = edge.getAttribute("data-to");
      if (!from || !to) return;
      (outOf[from] = outOf[from] || []).push(edge);
      (into[to] = into[to] || []).push(edge);
    });

    function walk(start, map, end, reached, lit) {
      var queue = [start];
      while (queue.length) {
        var column = queue.shift();
        (map[column] || []).forEach(function (edge) {
          lit.push(edge);
          var next = edge.getAttribute(end);
          if (!reached[next]) {
            reached[next] = true;
            queue.push(next);
          }
        });
      }
    }

    var traced = null;
    function trace(column) {
      svg.querySelectorAll(".on").forEach(function (el) { el.classList.remove("on"); });
      if (!column || column === traced) {
        traced = null;
        svg.classList.remove("tracing");
        return;
      }
      traced = column;
      var reached = {};
      reached[column] = true;
      var lit = [];
      walk(column, into, "data-from", reached, lit);
      walk(column, outOf, "data-to", reached, lit);
      lit.forEach(function (edge) { edge.classList.add("on"); });
      svg.querySelectorAll(".col").forEach(function (col) {
        if (reached[col.getAttribute("data-col")]) col.classList.add("on");
      });
      svg.classList.add("tracing");
    }

    svg.addEventListener("click", function (event) {
      var col = event.target.closest(".col");
      if (col) trace(col.getAttribute("data-col"));
    });
    svg.addEventListener("keydown", function (event) {
      var col = event.target.closest(".col");
      if (col && (event.key === "Enter" || event.key === " ")) {
        event.preventDefault();
        trace(col.getAttribute("data-col"));
      }
    });
    depth.addEventListener("change", filter);
    direction.addEventListener("change", filter);
    var reset = panel.querySelector("button.reset");
    if (reset) reset.addEventListener("click", function () { trace(null); });
    filter();

    // Open with the page's own table in view.
    var focus = svg.querySelector(".node.focus");
    var frame = panel.querySelector(".graph");
    if (focus && frame) {
      var box = focus.getBBox();
      var matrix = focus.transform.baseVal.consolidate();
      var x = matrix ? matrix.matrix.e : 0;
      frame.scrollLeft = Math.max(0, x + box.width / 2 - frame.clientWidth / 2);
    }

    function fromHash() {
      var match = /^#col=(.*)$/.exec(window.location.hash);
      if (!match) return;
      traced = null;
      trace(panel.getAttribute("data-focus") + "|" + decodeURIComponent(match[1]));
      panel.scrollIntoView({ block: "start" });
    }
    window.addEventListener("hashchange", fromHash);
    fromHash();
  });
})();
