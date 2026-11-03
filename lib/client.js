/**
 * dsh-speed-meter — WEB CLIENT half.
 *
 * Renders a live tok/s pill under the composer (`conversation.composer.dock`).
 * State source: SSE stream from the host half at /speed-meter/api/stream
 * (full snapshot pushed while it changes), with a fetch-polling fallback.
 */
window.__ModuleLoader__.load({
  id: "dsh-speed-meter",
  factory: (require) => {
    var module = { exports: {} };
    var exports = module.exports;
    Object.defineProperty(exports, Symbol.toStringTag, { value: "Module" });
    var React = require("react");
    var el = React.createElement;

    // ── store ──
    var listeners = new Set();
    var snap = { running: null, last: null, recent: [], calibrated: [] };
    // Transport diagnostics. A silent transport failure must never look like a
    // frozen reading: every state below is surfaced in the pill/tooltip.
    var link = { mode: "init", ok: false, base: "", httpStatus: 0, lastError: "", attempts: 0, lastPayloadAt: 0, sseStates: "" };

    function notify() { for (var fn of listeners) { try { fn(); } catch (e) {} } }
    function setSnap(next) {
      if (next && (next.running || next.last)) {
        link.ok = true
        link.lastPayloadAt = Date.now()
      }
      snap = next
      notify()
    }

    // Candidate base URLs for host routes. The desktop shell may serve the page
    // from a non-http origin, in which case a relative path never reaches the
    // host HTTP server; an explicit loopback origin (when known) is tried next.
    function bases() {
      var out = [""]
      try {
        if (window.__DSH_BOOT__ && window.__DSH_BOOT__.origin) out.push(String(window.__DSH_BOOT__.origin).replace(/\/$/, ""))
        if (window.__DSH_ORIGIN__) out.push(String(window.__DSH_ORIGIN__).replace(/\/$/, ""))
      } catch (e) {}
      try {
        if (typeof location !== "undefined" && location.origin && /^https?:/i.test(location.origin)) {
          var o = location.origin
          if (out.indexOf(o) < 0) out.push(o)
        }
      } catch (e) {}
      return out
    }

    var pollIv = null
    var baseIdx = 0

    function requestSnapshot() {
      var list = bases()
      if (baseIdx >= list.length) baseIdx = 0
      var base = list[baseIdx]
      link.attempts++
      return fetch(base + "/speed-meter/api", { cache: "no-store" })
        .then(function (r) {
          link.httpStatus = r.status
          if (!r.ok) throw new Error("HTTP " + r.status)
          return r.json()
        })
        .then(function (json) {
          link.mode = link.mode === "sse" ? "sse" : "poll"
          link.base = base
          link.lastError = ""
          setSnap(json)
        })
        .catch(function (e) {
          link.lastError = String((e && e.message) || e)
          if (baseIdx < list.length - 1) baseIdx++
        })
    }

    function startPolling() {
      if (pollIv) return
      pollIv = setInterval(requestSnapshot, 1200)
      requestSnapshot()
    }

    var sse = null
    function startSSE() {
      if (typeof EventSource === "undefined") { link.sseStates = "unavailable"; startPolling(); return }
      try {
        var es = new EventSource("/speed-meter/api/stream")
        es.onopen = function () { link.sseStates = "open" }
        es.onmessage = function (ev) {
          try { link.mode = "sse"; link.base = ""; link.sseStates = "message"; setSnap(JSON.parse(ev.data)) } catch (e) {}
        }
        es.onerror = function () {
          link.sseStates = "error"
          try { es.close() } catch (e) {}
          sse = null
          startPolling()
        }
        sse = es
      } catch (e) {
        link.sseStates = "throw"
        startPolling()
      }
    }
    // Both transports run: SSE for smooth updates, polling as the safety net
    // that also recovers after an SSE drop or a blocked stream.
    startSSE()
    startPolling()

    function useSnap() {
      var force = React.useState(0)[1];
      React.useEffect(function () {
        var fn = function () { force(function (n) { return n + 1; }); };
        listeners.add(fn);
        force(function (n) { return n + 1; });
        return function () { listeners.delete(fn); };
      }, []);
      return snap;
    }

    // ── formatting ──
    var STYLE = {
      pill: {
        display: "inline-flex", alignItems: "center", gap: 6,
        padding: "2px 10px", borderRadius: 999,
        fontSize: 12, lineHeight: "18px",
        border: "1px solid rgba(128,128,128,0.30)",
        background: "rgba(128,128,128,0.08)",
        color: "var(--color-text-secondary, rgba(128,128,128,1))",
        cursor: "pointer", userSelect: "none", whiteSpace: "nowrap",
        transition: "opacity .3s",
      },
      pillDim: { opacity: 0.55 },
      dot: {
        width: 7, height: 7, borderRadius: 999, display: "inline-block",
        background: "#3fb96f", boxShadow: "0 0 0 2px rgba(63,185,111,0.18)",
      },
      panel: {
        position: "absolute", bottom: "calc(100% + 6px)", left: 0, zIndex: 30,
        minWidth: 260, maxWidth: 340, padding: "8px 10px", borderRadius: 10,
        border: "1px solid rgba(128,128,128,0.30)",
        background: "var(--background-elevated, rgba(24,24,26,0.96))",
        color: "var(--color-text-primary, inherit)",
        boxShadow: "0 8px 24px rgba(0,0,0,0.25)",
        fontSize: 12,
      },
      row: { display: "flex", justifyContent: "space-between", gap: 12, padding: "2px 0" },
      dim: { opacity: 0.6 },
      muted: { opacity: 0.45 },
    };

    function fmt1(x) {
      if (x == null || !isFinite(x)) return "—";
      return (Math.round(x * 10) / 10).toFixed(1);
    }
    function fmtInt(x) { return x == null ? "—" : String(Math.round(x)).replace(/\B(?=(\d{3})+(?!\d))/g, ","); }
    function fmtMs(ms) { return ms == null ? "—" : (ms < 10000 ? (ms / 1000).toFixed(1) + "s" : Math.round(ms / 1000) + "s"); }
    function shortModel(m) { return m ? String(m).replace(/.*\//, "").replace(/-latest$/i, "") : "?"; }
    function purposeTag(p) {
      if (!p) return null;
      if (/compact|summary|标题|title/i.test(p)) return "·附加";
      return null;
    }

    var enT = {
      "运行中": "streaming", "等待首字": "waiting for first token",
      "上次调用": "last call", "近 12 次调用": "recent 12 calls",
      "生成 " : "gen ", " 次调用完成：每模型字节→token 比率自校准":" calls done: per-model chars→tokens ratio self-calibrated",
      "估值": "estimate", "精确": "exact", "净速率": "net rate", "含时延": "incl. TTFB",
    };
    function t(s) {
      try {
        var lang = (typeof navigator !== "undefined" && navigator.language) || "en";
        if (!/^zh/i.test(lang)) { return enT[s] || s; }
      } catch (e) {}
      return s;
    }

    function RecentPanel(onClose) {
      var rows = [];
      for (var i = 0; i < snap.recent.length; i++) {
        var r = snap.recent[i];
        rows.push(el("div", { key: i, style: STYLE.row },
          el("span", { style: STYLE.dim },
            new Date(r.finishedAt).toLocaleTimeString() + " " + shortModel(r.model) + (purposeTag(r.purpose) ? " " + purposeTag(r.purpose) : "")),
          el("span", { style: { fontVariantNumeric: "tabular-nums" } },
            (r.tokSec != null ? fmt1(r.tokSec) + " tok/s" : "—") + " · " + fmtInt(r.tokens) + " · " + fmtMs(r.genMs))
        ));
      }
      if (!rows.length) rows.push(el("div", { style: STYLE.muted }, "—"));
      var calib = snap.calibrated.map(function (c, i) {
        return el("div", { key: "c" + i, style: Object.assign({}, STYLE.row, STYLE.muted) },
          el("span", null, "⚡模型 " + shortModel(c.model)),
          el("span", null, "chars→tok " + fmt1(1 / c.ratio) + "（" + c.n + " 次校准）"));
      });
      var lastLine = snap.last
        ? el("div", { style: Object.assign({}, STYLE.row, { borderTop: "1px solid rgba(128,128,128,0.25)", marginTop: 4, paddingTop: 4 }) },
            el("span", { style: STYLE.dim }, t("上次调用")),
            el("span", { style: { fontVariantNumeric: "tabular-nums" } },
              (snap.last.tokSec != null ? fmt1(snap.last.tokSec) + " tok/s · " : "— · ") +
              fmtInt(snap.last.tokens) + " tok · " + fmtMs(snap.last.genMs) +
              (snap.last.exact ? "" : " " + t("估值")))
          )
        : null;
      return el("div", { style: STYLE.panel, onClick: function (e) { e.stopPropagation(); onClose(); } },
        rows, lastLine, calib.length ? calib : null,
        el("div", { style: Object.assign({}, STYLE.muted, { marginTop: 4 }) },
          "" + (snap.recent.length || 0) + t(" 次调用完成：每模型字节→token 比率自校准")));
    }

    function SpeedMeter() {
      var s = useSnap();
      var openState = React.useState(false);
      var isOpen = openState[0];
      var setOpen = openState[1];

      var runningData = s && s.running;
      var lastData = s && s.last;
      var active = !!(runningData && !runningData.waiting);
      var waiting = !!(runningData && runningData.waiting);

      var age = link.lastPayloadAt ? Date.now() - link.lastPayloadAt : -1;
      var stale = link.ok && age > 8000;

      // Nothing from the host yet: say so instead of silently rendering nothing
      // (a silent transport failure used to look like a frozen gauge).
      if (!runningData && !lastData) {
        if (!link.ok && link.attempts > 1) {
          return el("div", { style: Object.assign({}, STYLE.pill, STYLE.pillDim), title: diagText() },
            el("span", { style: Object.assign({}, STYLE.dot, { background: "#c8552e", boxShadow: "none" }) }),
            el("span", null, "⚡ " + t("未连上宿主") + " · " + (link.httpStatus ? "HTTP " + link.httpStatus : (link.lastError || link.mode)))
          )
        }
        return null;
      }

      var label, title, dim = false;
      if (active) {
        var rate = runningData.windowTokSec != null ? runningData.windowTokSec : runningData.tokSec;
        label = "⚡ " + fmt1(rate) + " tok/s";
        title = [
          "sessionId: " + (runningData.sessionId || "?"),
          "model: " + runningData.model + (runningData.purpose ? "  ·  purpose: " + runningData.purpose : ""),
          runningData.windowTokSec != null ? "instant(1.2s): " + fmt1(runningData.windowTokSec) + " tok/s" : null,
          "avg(gen): " + fmt1(runningData.tokSec) + " tok/s · est ~" + fmtInt(runningData.estTokens) + " tok · chars " + fmtInt(runningData.chars) + " · ratio " + runningData.ratio,
          "elapsed: " + fmtMs(runningData.genMs),
          "ratio = 精确 outputTokens/chars 的逐次 EMA；估算在流结束瞬间被精确值替换。",
        ].filter(Boolean).join("\n") + "\n" + diagText();
      } else if (waiting) {
        label = "⚡ " + t("等待首字");
        title = (runningData.model ? "model: " + runningData.model + "\n" : "") + diagText();
        dim = true;
      } else {
        var l = lastData;
        var hasRate = l.tokSec != null;
        label = hasRate
          ? "⚡ " + fmt1(l.tokSec) + " tok/s · " + fmtInt(l.tokens) + " tok"
          : "⚡ " + fmtInt(l.tokens) + " tok · " + fmtMs(l.genMs);
        if (stale) label += " · " + t("已停");
        dim = true;
        title = [
          t("上次调用") + ": " + l.model + (l.purpose ? "  ·  " + l.purpose : ""),
          hasRate
            ? (l.exact ? t("精确") + " 速率" : t("估值")) + ": " + fmt1(l.tokSec) + " tok/s"
            : "本次调用短于测量阈值或为单突发（tool-call 参数 / 缓存回放），时间分母不代表生成速率，仅给出 token 数。",
          "tokens: " + fmtInt(l.tokens) + " · chars: " + fmtInt(l.chars) + " · " + fmtMs(l.genMs),
          stale ? "⚠ 数据已 " + Math.round(age / 1000) + "s 未更新（连接可能已断）" : null,
          nts(s.recent),
          diagText(),
        ].filter(Boolean).join("\n");
      }

      return el("div", { style: { position: "relative", display: "inline-block" } },
        el("div", {
          style: Object.assign({}, STYLE.pill, dim ? STYLE.pillDim : null),
          title: title,
          onClick: function () { setOpen(!isOpen); },
        },
          el("span", { style: Object.assign({}, STYLE.dot, active ? null : { background: stale ? "#c8552e" : "rgba(128,128,128,0.6)", boxShadow: "none" }) }),
          el("span", { style: { fontVariantNumeric: "tabular-nums", color: active ? "#2ea860" : undefined } }, label)
        ),
        isOpen ? RecentPanel(function () { setOpen(false); }) : null
      );
    }

    function diagText() {
      var age = link.lastPayloadAt ? Date.now() - link.lastPayloadAt : -1;
      return "— 传输 —\nmode: " + link.mode + (link.base ? "  base: " + (link.base || "(same-origin)") : "") +
        "\nok: " + link.ok + "  attempts: " + link.attempts + "  http: " + (link.httpStatus || "-") +
        "\nsse: " + (link.sseStates || "-") +
        "\nlast payload: " + (age < 0 ? "never" : Math.round(age / 1000) + "s ago") +
        (link.lastError ? "\nlast error: " + link.lastError : "");
    }

    function nts(arr) {
      if (!arr || !arr.length) return "";
      return "\n" + t("近 12 次调用") + ": " + arr.length;
    }

    // ── plugin ──
    var inject = ["slots"];

    function apply(ctx) {
      var slots = ctx.get("slots");
      if (slots === undefined || slots.inject === undefined) return;
      slots.inject("conversation.composer.dock", function () {
        return slots.register(
          { name: "conversation.composer.dock", id: "speed-meter", order: 10, label: "speed-meter" },
          function () { return el("div", { style: { display: "inline-flex" } }, el(SpeedMeter, null)); }
        );
      });
    }

    exports.inject = inject;
    exports.apply = apply;
    // test/debug surface (same spirit as dsh-usage-plugin's exports.__i18n):
    // lets a harness push snapshots without a live SSE connection.
    exports.__store = { set: setSnap, get: function () { return snap; } };
    exports.__link = link;
    return module.exports;
  }
});
