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
    // Session the dock is mounted in: the pill must never show another
    // window's numbers, so every request carries it and a switch resets state.
    var session = ""

    function withSession(path) {
      return session ? path + "?session=" + encodeURIComponent(session) : path
    }

    function requestSnapshot() {
      var list = bases()
      if (baseIdx >= list.length) baseIdx = 0
      var base = list[baseIdx]
      link.attempts++
      return fetch(base + withSession("/speed-meter/api"), { cache: "no-store" })
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
    function closeSSE() {
      if (!sse) return
      try { sse.close() } catch (e) {}
      sse = null
      link.sseStates = "closed"
    }
    function startSSE() {
      if (typeof EventSource === "undefined") { link.sseStates = "unavailable"; startPolling(); return }
      closeSSE()
      try {
        var es = new EventSource(withSession("/speed-meter/api/stream"))
        es.onopen = function () { link.sseStates = "open" }
        es.onmessage = function (ev) {
          try { link.mode = "sse"; link.base = ""; link.sseStates = "message"; setSnap(JSON.parse(ev.data)) } catch (e) {}
        }
        es.onerror = function () {
          link.sseStates = "error"
          try { es.close() } catch (e) {}
          if (sse === es) sse = null
          startPolling()
        }
        sse = es
      } catch (e) {
        link.sseStates = "throw"
        startPolling()
      }
    }

    /** Bind the dock to a session; a switch drops everything and reconnects. */
    function setSession(next) {
      var id = next == null ? "" : String(next)
      if (id === session) return
      session = id
      snap = { running: null, last: null, recent: [], calibrated: [] }
      link.ok = false
      link.attempts = 0
      link.httpStatus = 0
      link.lastError = ""
      link.lastPayloadAt = 0
      notify()
      startSSE()
      requestSnapshot()
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
      "本轮": "turn", "次": " calls", "本轮会话": "turn", "次调用": " model calls",
      "本轮平均": "turn average", "本轮累计": "turn so far", "本轮用量": "turn usage",
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
      var tv = snap.turn;
      var turnLine = tv && tv.calls > 0
        ? el("div", { style: Object.assign({}, STYLE.row, { borderBottom: "1px solid rgba(128,128,128,0.25)", marginBottom: 4, paddingBottom: 4 }) },
            el("span", null, "⚡ " + t("本轮") + (tv.active ? "（进行中）" : "")),
            el("span", { style: { fontVariantNumeric: "tabular-nums" } },
              fmt1(tv.tokSec) + " tok/s · " + fmtInt(tv.tokens) + " tok · " + tv.calls + t("次")))
        : null;
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
        turnLine, rows, lastLine, calib.length ? calib : null,
        el("div", { style: Object.assign({}, STYLE.muted, { marginTop: 4 }) },
          "" + (snap.recent.length || 0) + t(" 次调用完成：每模型字节→token 比率自校准")));
    }


    // ── client-side derivation (works for LOCAL and REMOTE sessions) ──────────
    // In Remote client mode the conversation itself is served from the remote
    // Host through the Typert gateway, so the dock receives the same `useChat`
    // snapshot the official statistics pills read. When the local host half has
    // no data for this session (remote mode, or no host plugin), the pill falls
    // back to deriving the numbers from that snapshot: exact per-step usage for
    // finished steps, character growth for the in-flight step.
    var EST_TOKENS_PER_CHAR = 0.34
    var liveTrackers = {}

    function blockChars(blocks) {
      var n = 0
      if (!blocks || !blocks.length) return n
      for (var i = 0; i < blocks.length; i++) {
        var b = blocks[i]
        if (!b) continue
        if (b.type === "text" || b.type === "reasoning") n += String(b.text || "").length
        else if (b.type === "tool-call") n += String(b.arguments || b.argumentsDelta || "").length
      }
      return n
    }

    /** Instant rate for the in-flight step, measured from snapshot growth. */
    function liveRate(sid, partial) {
      var now = Date.now()
      var key = sid + "|" + partial.turn + "|" + partial.step
      var chars = blockChars(partial.blocks)
      var t = liveTrackers[key]
      if (!t || chars < t.chars) t = liveTrackers[key] = { chars: chars, at: now, series: [[now, chars]] }
      if (chars > t.chars) { t.chars = chars; t.at = now; t.series.push([now, chars]) }
      var cutoff = now - 1600
      while (t.series.length > 1 && t.series[0][0] < cutoff) t.series.shift()
      var span = now - t.series[0][0]
      var grown = chars - t.series[0][1]
      return {
        chars: chars,
        rate: span >= 250 && grown > 0 ? Math.round(grown * EST_TOKENS_PER_CHAR * 1000 / span * 10) / 10 : null,
        idleMs: now - t.at,
      }
    }

    function deriveTurn(legacy) {
      var nodes = (legacy && legacy.nodes) || []
      var byTurn = {}
      var lastTurn = null
      for (var i = 0; i < nodes.length; i++) {
        var node = nodes[i]
        if (!node || node.kind !== "assistant") continue
        var turn = node.turn
        var e = byTurn[turn] || (byTurn[turn] = { tokens: 0, decodeMs: 0, calls: 0 })
        var timing = node.timing || {}
        var toks = node.usage && typeof node.usage.outputTokens === "number" ? node.usage.outputTokens : null
        if (toks !== null) { e.tokens += toks; e.calls += 1 }
        if (timing.firstTokenTime != null && timing.completedTime != null) {
          e.decodeMs += Math.max(0, timing.completedTime - timing.firstTokenTime)
        }
        if (lastTurn === null || turn > lastTurn) lastTurn = turn
      }
      var partial = legacy && legacy.partial
      if (partial && partial.turn != null && (lastTurn === null || partial.turn > lastTurn)) lastTurn = partial.turn
      var turn = lastTurn !== null && byTurn[lastTurn] ? byTurn[lastTurn] : null
      return { lastTurn: lastTurn, turn: turn, partial: partial || null }
    }

    /** Pill fed only by the conversation snapshot — no host HTTP needed. */
    function DerivedPill(props) {
      var sid = props.sid || ""
      var legacy = props.useChat(function (s) { return s && s.legacy ? s.legacy : null })
      var proj = props.useProjection ? props.useProjection("sessionStats") : undefined
      var d = deriveTurn(legacy)
      var live = d.partial ? liveRate(sid, d.partial) : null
      var sessionLine = proj && proj.decodeMs > 0 && proj.decodeTokens > 0
        ? "会话累计: " + fmtInt(proj.decodeTokens) + " tok · " + fmt1(proj.decodeTokens * 1000 / proj.decodeMs) + " tok/s（" + fmtInt(proj.turns) + " 轮）"
        : null

      if (live && (live.rate != null || (d.partial && d.partial.step != null))) {
        var label = live.rate != null
          ? "⚡ " + fmt1(live.rate) + " tok/s"
          : "⚡ " + t("生成中")
        var title = [
          "数据来源：会话快照（客户端派生，" + (props.remoteHint ? "远端" : "本地") + "）",
          "turn " + (d.partial.turn != null ? d.partial.turn : "?") + " · step " + (d.partial.step != null ? d.partial.step : "?"),
          live.rate != null
            ? "instant: " + fmt1(live.rate) + " tok/s（按字符增长 × " + EST_TOKENS_PER_CHAR + " tokens/char 估算）"
            : "已收到该步内容，等待更多增量后显示瞬时速率",
          "本步字符: " + fmtInt(live.chars),
          d.turn ? "上一已完成步: " + fmtInt(d.turn.tokens) + " tok · " + fmt1(d.turn.tokens * 1000 / Math.max(1, d.turn.decodeMs)) + " tok/s" : null,
          sessionLine,
        ].filter(Boolean).join("\n")
        return el("div", { style: Object.assign({}, STYLE.pill, STYLE.pillDim), title: title },
          el("span", { style: Object.assign({}, STYLE.dot, { background: "#2ea860", boxShadow: "0 0 0 2px rgba(46,168,96,0.18)" }) }),
          el("span", { style: { fontVariantNumeric: "tabular-nums" } }, label))
      }

      if (d.turn && d.turn.calls > 0 && d.turn.tokens > 0) {
        var tokSec = d.turn.tokens * 1000 / Math.max(1, d.turn.decodeMs)
        var lbl = "⚡ " + t("本轮") + " " + fmt1(tokSec) + " tok/s · " + fmtInt(d.turn.tokens) + " tok · " + d.turn.calls + t("次")
        var ttl = [
          "数据来源：会话快照（客户端派生，" + (props.remoteHint ? "远端" : "本地") + "）",
          t("本轮会话") + "：turn " + d.lastTurn + " · " + fmtInt(d.turn.tokens) + " tok / " + d.turn.calls + t("次调用") + " · 净生成 " + fmtMs(d.turn.decodeMs),
          t("本轮平均") + "：" + fmt1(tokSec) + " tok/s（精确：outputTokens ÷ (completedTime − firstTokenTime)）",
          sessionLine,
        ].filter(Boolean).join("\n")
        return el("div", { style: Object.assign({}, STYLE.pill, STYLE.pillDim), title: ttl },
          el("span", { style: Object.assign({}, STYLE.dot, { background: "rgba(128,128,128,0.6)", boxShadow: "none" }) }),
          el("span", { style: { fontVariantNumeric: "tabular-nums" } }, lbl))
      }
      return null
    }

    function SpeedMeter(props) {
      var s = useSnap();
      var openState = React.useState(false);
      var isOpen = openState[0];
      var setOpen = openState[1];

      // Bind to the session this dock lives in (dock props expose sessionId).
      var sid = props && props.sessionId != null ? String(props.sessionId) : "";
      React.useEffect(function () { setSession(sid); }, [sid]);

      var runningData = s && s.running;
      var lastData = s && s.last;
      var active = !!(runningData && !runningData.waiting);
      var waiting = !!(runningData && runningData.waiting);

      var age = link.lastPayloadAt ? Date.now() - link.lastPayloadAt : -1;
      var stale = link.ok && age > 8000;

      // Nothing from the host yet: say so instead of silently rendering nothing
      // (a silent transport failure used to look like a frozen gauge).
      if (!runningData && !lastData) {
        // Remote client mode (or a missing host plugin): derive from the
        // conversation snapshot the dock already receives.
        if (props && typeof props.useChat === "function") {
          return el(DerivedPill, { useChat: props.useChat, useProjection: props.useProjection, sid: sid, remoteHint: !!(s && s.source === "remote") })
        }
        if (!link.ok && link.attempts > 1) {
          return el("div", { style: Object.assign({}, STYLE.pill, STYLE.pillDim), title: diagText() },
            el("span", { style: Object.assign({}, STYLE.dot, { background: "#c8552e", boxShadow: "none" }) }),
            el("span", null, "⚡ " + t("未连上宿主") + " · " + (link.httpStatus ? "HTTP " + link.httpStatus : (link.lastError || link.mode)))
          )
        }
        return null;
      }

      var elsewhere = s && s.runningElsewhere;
      var elsewhereLine = elsewhere
        ? "其他会话正在生成（" + shortModel(elsewhere.model) + " · 本会话不显示）"
        : null;

      var label, title, dim = false;
      if (active) {
        var rate = runningData.windowTokSec != null ? runningData.windowTokSec : runningData.tokSec;
        label = "⚡ " + fmt1(rate) + " tok/s" + (s && s.source === "remote" ? " ⇅" : "");
        var tvLive = s && s.turn;
        title = [
          "sessionId: " + (runningData.sessionId || "?"),
          "model: " + runningData.model + (runningData.purpose ? "  ·  purpose: " + runningData.purpose : ""),
          runningData.windowTokSec != null ? "instant(1.2s): " + fmt1(runningData.windowTokSec) + " tok/s" : null,
          "avg(gen): " + fmt1(runningData.tokSec) + " tok/s · est ~" + fmtInt(runningData.estTokens) + " tok · chars " + fmtInt(runningData.chars) + " · ratio " + runningData.ratio,
          "elapsed: " + fmtMs(runningData.genMs),
          tvLive && tvLive.calls > 0
            ? t("本轮累计") + ": " + fmtInt(tvLive.tokens) + " tok / " + tvLive.calls + t("次调用") + "（" + fmt1(tvLive.tokSec) + " tok/s）"
            : null,
          "ratio = 精确 outputTokens/chars 的逐次 EMA；估算在流结束瞬间被精确值替换。",
          elsewhereLine,
        ].filter(Boolean).join("\n") + "\n" + diagText();
      } else if (waiting) {
        label = "⚡ " + t("等待首字");
        title = (runningData.model ? "model: " + runningData.model + "\n" : "") + diagText();
        dim = true;
      } else {
        // Turn first: once a turn ends, the pill reports that turn's average
        // speed and cumulative usage instead of the last single call.
        var tv = s && s.turn;
        var l = lastData;
        var hasRate = l && l.tokSec != null;
        var turnLabel = null;
        if (tv && tv.calls > 0 && !tv.active) {
          turnLabel = "⚡ " + t("本轮") + " " + fmt1(tv.tokSec) + " tok/s · " + fmtInt(tv.tokens) + " tok · " + tv.calls + t("次");
        }
        if (turnLabel) {
          label = turnLabel;
          if (stale) label += " · " + t("已停");
          dim = true;
          title = [
            t("本轮会话") + "：" + fmtInt(tv.tokens) + " tok / " + tv.calls + t("次调用") +
              " · " + fmtMs(tv.startedAt ? tv.endedAt - tv.startedAt : null),
            t("本轮平均") + "：" + fmt1(tv.tokSec) + " tok/s" +
              (tv.exact ? "（" + t("精确") + "）" : "（" + t("估值") + "）"),
            "tokens: " + fmtInt(tv.tokens) + " · chars: " + fmtInt(tv.chars) + " · 生成耗时 " + fmtMs(tv.genMs) +
              " · 墙钟 " + fmtMs(tv.wallMs) + (tv.wallTokSec != null ? "（含工具等待 " + fmt1(tv.wallTokSec) + " tok/s）" : ""),
            tv.models && tv.models.length ? "model: " + tv.models.join(", ") : null,
            l ? t("上次调用") + ": " + fmt1(l.tokSec) + " tok/s · " + fmtInt(l.tokens) + " tok" : null,
            stale ? "⚠ 数据已 " + Math.round(age / 1000) + "s 未更新（连接可能已断）" : null,
            elsewhereLine,
            nts(s.recent),
            diagText(),
          ].filter(Boolean).join("\n");
        } else {
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
            elsewhereLine,
            nts(s.recent),
            diagText(),
          ].filter(Boolean).join("\n");
        }
      }

      if (s && s.source === "remote") {
        if (label.indexOf("⇅") < 0) label += " ⇅"
        title = "数据来源：远端 Host（preview 代理）\n" + title
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
        (link.lastError ? "\nlast error: " + link.lastError : "") +
        (snap && snap.source ? "\nsource: " + snap.source : "") +
        (snap && snap.remote ? "\nremote: " + JSON.stringify(snap.remote) : "");
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
          // dock entries receive the session-scoped standard props; sessionId
          // scopes the pill to the conversation it is mounted in.
          function (props) {
            return el("div", { style: { display: "inline-flex" } },
              el(SpeedMeter, {
                sessionId: props && props.sessionId,
                useChat: props && props.useChat,
                useProjection: props && props.useProjection,
              }));
          }
        );
      });
    }

    exports.inject = inject;
    exports.apply = apply;
    // test/debug surface (same spirit as dsh-usage-plugin's exports.__i18n):
    // lets a harness push snapshots without a live SSE connection.
    exports.__store = { set: setSnap, get: function () { return snap; } };
    exports.__link = link;
    exports.__session = { set: setSession, get: function () { return session; } };
    return module.exports;
  }
});
