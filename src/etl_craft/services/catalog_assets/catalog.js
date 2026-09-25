/* The etl-craft catalog: search, and the lineage graph's filters and column tracing. */
(function () {
  "use strict";

  var root = document.body.getAttribute("data-root") || "";

  // Run details are as of when the site was generated: say so once they are over a day old.
  var generated = Date.parse(document.body.getAttribute("data-generated") || "");
  var hours = (Date.now() - generated) / 3600000;
  if (hours > 26) {
    var note = document.createElement("p");
    note.className = "stale";
    note.textContent = "The run details on this site are " +
      (hours < 48 ? Math.round(hours) + " hours" : Math.round(hours / 24) + " days") +
      " old: it was generated " + new Date(generated).toLocaleString() +
      " and has not been written again since.";
    var main = document.querySelector("main");
    if (main) main.insertBefore(note, main.firstChild);
  }
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

  // The lineage graph. Tables open collapsed, the page's own table expanded; clicking a
  // table's header shows or hides its columns, and the boxes are stacked again to fit. The
  // drawing can be filtered by direction and depth, and a column traced through every path.
  // Zoom scales a graph inside its frame; the frame scrolls, dragging its background pans, and
  // Ctrl or Cmd with the wheel zooms around the pointer.
  function panZoom(panel, svg) {
    var frame = panel.querySelector(".graph");
    var box = svg.getAttribute("viewBox").split(" ");
    var size = { width: parseFloat(box[2]), height: parseFloat(box[3]) };
    var zoom = 1;

    function zoomTo(scale, pointX, pointY) {
      var before = zoom;
      zoom = Math.min(2.5, Math.max(0.2, scale));
      svg.setAttribute("width", size.width * zoom);
      svg.setAttribute("height", size.height * zoom);
      if (frame && pointX !== undefined) {
        var ratio = zoom / before;
        frame.scrollLeft = (frame.scrollLeft + pointX) * ratio - pointX;
        frame.scrollTop = (frame.scrollTop + pointY) * ratio - pointY;
      }
    }

    function fit() {
      if (!frame || !size.width) return;
      zoomTo(Math.min(1, (frame.clientWidth - 4) / size.width,
        (frame.clientHeight - 4) / size.height));
    }

    if (frame) {
      frame.addEventListener("wheel", function (event) {
        if (!event.ctrlKey && !event.metaKey) return;
        event.preventDefault();
        var at = frame.getBoundingClientRect();
        zoomTo(zoom * (event.deltaY < 0 ? 1.1 : 1 / 1.1),
          event.clientX - at.left, event.clientY - at.top);
      }, { passive: false });
      var drag = null;
      frame.addEventListener("pointerdown", function (event) {
        if (event.button !== 0 || event.target.closest(".col, .head, .open, a")) return;
        drag = { x: event.clientX, y: event.clientY, left: frame.scrollLeft, top: frame.scrollTop };
        frame.classList.add("panning");
        frame.setPointerCapture(event.pointerId);
      });
      frame.addEventListener("pointermove", function (event) {
        if (!drag) return;
        frame.scrollLeft = drag.left - (event.clientX - drag.x);
        frame.scrollTop = drag.top - (event.clientY - drag.y);
      });
      var stop = function () { drag = null; frame.classList.remove("panning"); };
      frame.addEventListener("pointerup", stop);
      frame.addEventListener("pointercancel", stop);
    }
    var buttons = {
      "zoom-in": function () { zoomTo(zoom * 1.25); },
      "zoom-out": function () { zoomTo(zoom / 1.25); },
      "zoom-fit": fit
    };
    Object.keys(buttons).forEach(function (name) {
      var button = panel.querySelector("button." + name);
      if (button) button.addEventListener("click", buttons[name]);
    });
    return {
      zoom: function () { return zoom; },
      resize: function (width, height) {
        size.width = width;
        size.height = height;
        zoomTo(zoom);
      }
    };
  }

  document.querySelectorAll(".dag-panel").forEach(function (panel) {
    var svg = panel.querySelector("svg.dag");
    if (svg) panZoom(panel, svg);
  });

  document.querySelectorAll(".lineage-panel").forEach(function (panel) {
    var svg = panel.querySelector("svg.lineage");
    if (!svg) return;
    var number = function (name) { return parseFloat(svg.getAttribute(name)); };
    var WIDTH = number("data-node-width");
    var HEADER = number("data-header");
    var ROW = number("data-row");
    var GAP = number("data-gap");
    var MARGIN = number("data-margin");
    var depth = panel.querySelector("select.depth");
    var direction = panel.querySelector("select.direction");
    var nodes = Array.prototype.slice.call(svg.querySelectorAll(".node"));
    var edges = Array.prototype.slice.call(svg.querySelectorAll(".edge"));
    var frame = panel.querySelector(".graph");
    var at = {};
    var view = panZoom(panel, svg);

    function expanded(node) { return !node.classList.contains("collapsed"); }

    function rows(node) {
      return node.querySelectorAll(".col").length + (node.querySelector(".more") ? 1 : 0);
    }

    function height(node) {
      var count = expanded(node) ? rows(node) : 0;
      return HEADER + ROW * count + (count ? 6 : 0);
    }

    function expand(node, open) {
      node.classList.toggle("collapsed", !open);
      var chevron = node.querySelector(".chevron");
      if (chevron) chevron.textContent = open ? "▾" : "▸";
      node.querySelector(".box").setAttribute("height", height(node));
    }

    function anchor(place, column) {
      if (column && expanded(place.node)) {
        var cols = place.node.querySelectorAll(".col");
        for (var i = 0; i < cols.length; i++) {
          if (cols[i].getAttribute("data-col") === column) {
            return place.y + HEADER + ROW * i + ROW / 2;
          }
        }
      }
      return place.y + HEADER / 2;
    }

    function curve(x1, y1, x2, y2) {
      var bend = Math.max(40, Math.abs(x2 - x1) / 2);
      return "M" + x1 + "," + y1 + " C" + (x1 + bend) + "," + y1 + " " + (x2 - bend) + "," +
        y2 + " " + x2 + "," + y2;
    }

    // Stack the shown boxes of each level in their drawn order, centred on the tallest level,
    // then run every edge from its column's row, or its table's header when it is collapsed.
    function layout() {
      var levels = {};
      nodes.forEach(function (node) {
        if (node.classList.contains("hidden")) return;
        var level = node.getAttribute("data-level");
        (levels[level] = levels[level] || []).push(node);
      });
      var tallest = 0;
      var heights = {};
      Object.keys(levels).forEach(function (level) {
        levels[level].sort(function (a, b) {
          return parseFloat(a.getAttribute("data-y")) - parseFloat(b.getAttribute("data-y"));
        });
        var total = GAP * (levels[level].length - 1);
        levels[level].forEach(function (node) { total += height(node); });
        heights[level] = total;
        tallest = Math.max(tallest, total);
      });
      at = {};
      Object.keys(levels).forEach(function (level) {
        var y = MARGIN + (tallest - heights[level]) / 2;
        levels[level].forEach(function (node) {
          var x = parseFloat(node.getAttribute("data-x"));
          node.setAttribute("transform", "translate(" + x + "," + y + ")");
          at[node.getAttribute("data-table")] = { node: node, x: x, y: y };
          y += height(node) + GAP;
        });
      });
      var full = svg.getAttribute("viewBox").split(" ");
      var tall = tallest + 2 * MARGIN;
      svg.setAttribute("viewBox", "0 0 " + full[2] + " " + tall);
      view.resize(parseFloat(full[2]), tall);
      edges.forEach(function (edge) {
        var source = at[edge.getAttribute("data-source")];
        var target = at[edge.getAttribute("data-target")];
        if (!source || !target) return;
        edge.setAttribute("d", curve(
          source.x + WIDTH, anchor(source, edge.getAttribute("data-from")),
          target.x, anchor(target, edge.getAttribute("data-to"))
        ));
      });
    }

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
      layout();
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

    function light(edge, on) {
      edge.classList.toggle("on", on);
      edge.setAttribute("marker-end", on ? "url(#arrow-on)" : "url(#arrow)");
    }

    // Trace a column: every path into it and out of it, with each table on the way expanded.
    var traced = null;
    function trace(column) {
      edges.forEach(function (edge) { light(edge, false); });
      svg.querySelectorAll(".col.on").forEach(function (el) { el.classList.remove("on"); });
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
      Object.keys(reached).forEach(function (col) {
        var place = at[col.split("|")[0]];
        if (place && !expanded(place.node)) expand(place.node, true);
      });
      layout();
      lit.forEach(function (edge) { light(edge, true); });
      svg.querySelectorAll(".col").forEach(function (col) {
        if (reached[col.getAttribute("data-col")]) col.classList.add("on");
      });
      svg.classList.add("tracing");
    }

    function toggle(head) {
      var node = head.closest(".node");
      expand(node, !expanded(node));
      layout();
    }

    svg.addEventListener("click", function (event) {
      var col = event.target.closest(".col");
      if (col) return trace(col.getAttribute("data-col"));
      var head = event.target.closest(".head");
      if (head) toggle(head);
    });
    svg.addEventListener("keydown", function (event) {
      if (event.key !== "Enter" && event.key !== " ") return;
      var col = event.target.closest(".col");
      var head = event.target.closest(".head");
      if (!col && !head) return;
      event.preventDefault();
      if (col) trace(col.getAttribute("data-col")); else toggle(head);
    });
    depth.addEventListener("change", filter);
    direction.addEventListener("change", filter);
    var buttons = {
      reset: function () { trace(null); },
      "expand-all": function () { nodes.forEach(function (n) { expand(n, true); }); layout(); },
      "collapse-all": function () {
        nodes.forEach(function (n) { expand(n, n.classList.contains("focus")); });
        layout();
      }
    };
    Object.keys(buttons).forEach(function (name) {
      var button = panel.querySelector("button." + name);
      if (button) button.addEventListener("click", buttons[name]);
    });

    nodes.forEach(function (node) { expand(node, node.classList.contains("focus")); });
    filter();

    // Open with the page's own table in view.
    var focus = at[panel.getAttribute("data-focus")];
    if (focus && frame) {
      frame.scrollLeft = Math.max(0, (focus.x + WIDTH / 2) * view.zoom() - frame.clientWidth / 2);
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
