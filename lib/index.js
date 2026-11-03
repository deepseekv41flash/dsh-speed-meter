/**
 * dsh-speed-meter — HOST half.
 *
 * Cordis plugin for a DeepSeek Harness web/desktop profile:
 *  - wraps every streaming model call via the `llm/stream` waterfall,
 *  - counts text / reasoning / tool-call argument deltas as they flow,
 *  - self-calibrates a chars→tokens ratio per model from the final `usage`
 *    chunk (exact outputTokens / counted chars), so the live estimate
 *    converges to the real billing-relevant output tokens,
 *  - serves a JSON snapshot at `GET /speed-meter/api` and an SSE stream at
 *    `GET /speed-meter/api/stream` (full snapshot pushed while it changes).
 *
 * No filesystem or external service dependencies. All state is in memory;
 * a web/desktop DSH restart resets it, which is acceptable for a live meter.
 */

/** Sliding window (ms) used for the "instant" rate shown while streaming. */
const WINDOW_MS = 1200
/** Minimum window span (ms) before the instant rate is trusted. */
const WINDOW_MIN_MS = 250
/** Min chars before a finished call may calibrate the model ratio. */
const CALIBRATE_MIN_CHARS = 200
/** Max recent-call rows served to the client half. */
const RECENT_KEEP = 12

function safeGet(obj, key) {
  try {
    return obj ? obj[key] : undefined
  } catch (e) {
    return undefined
  }
}

export default {
  apply(ctx) {
    let webServer
    try { webServer = typeof ctx.get === 'function' ? ctx.get('webServer') : undefined } catch (e) { webServer = undefined }
    if (webServer === undefined) webServer = safeGet(ctx, 'webServer')

    /** Running streaming call, or null. */
    let running = null
    /** Last completed call summary (exact rate when usage was received). */
    let last = null
    /** Recent completed calls, newest first. */
    const recent = []
    /** model → { ratio, n } EMA of outputTokens / countedChars. */
    const ratioByModel = new Map()

    function modelRatio(model) {
      const r = ratioByModel.get(model)
      return r ? r.ratio : 3.6
    }

    function countedChars(run) {
      return run.textChars + run.reasoningChars + run.toolChars
    }

    // ── llm/stream tap ──────────────────────────────────────────────────────
    ctx.on('llm/stream', function handleLlmStream(options, next) {
      const source = next()
      if (!options || typeof options.model !== 'string') return source
      const model = options.model
      const provider = options.provider || ''
      const purpose = options.purpose ? String(options.purpose) : ''
      let sessionId = options.sessionId ? String(options.sessionId) : ''

      const run = {
        sessionId,
        model,
        provider,
        purpose,
        startedAt: Date.now(),
        firstDeltaAt: 0,
        lastDeltaAt: 0,
        textChars: 0,
        reasoningChars: 0,
        toolChars: 0,
        points: [],
        usage: null,
        finishKind: '',
      }
      running = run

      async function* observe() {
        try {
          for await (const chunk of source) {
            if (chunk && typeof chunk === 'object') {
              const now = Date.now()
              if (chunk.type === 'text-delta' && chunk.text) {
                if (!run.firstDeltaAt) run.firstDeltaAt = now
                run.lastDeltaAt = now
                run.textChars += chunk.text.length
              } else if (chunk.type === 'reasoning-delta' && chunk.text) {
                if (!run.firstDeltaAt) run.firstDeltaAt = now
                run.lastDeltaAt = now
                run.reasoningChars += chunk.text.length
              } else if (chunk.type === 'tool-call-delta' && chunk.argumentsDelta) {
                if (!run.firstDeltaAt) run.firstDeltaAt = now
                run.lastDeltaAt = now
                run.toolChars += chunk.argumentsDelta.length
              } else if (chunk.type === 'block-start' || chunk.type === 'block-end') {
                // structure only, no text
              } else if (chunk.type === 'usage' && chunk.usage) {
                run.usage = chunk.usage
              } else if (chunk.type === 'finish') {
                const r = chunk.reason
                run.finishKind = r ? String(r.kind || '') : ''
              }
            }
            yield chunk
          }
        } finally {
          finalize(run)
          if (running === run) running = null
        }
      }
      return observe()
    })

    function finalize(run) {
      const chars = countedChars(run)
      const now = Date.now()
      const endAt = run.lastDeltaAt || now
      const genMs = run.firstDeltaAt ? Math.max(1, endAt - run.firstDeltaAt) : Math.max(1, now - run.startedAt)

      // exact token count when the adapter reported usage; otherwise estimate
      const tokens = run.usage && typeof run.usage.outputTokens === 'number'
        ? run.usage.outputTokens
        : Math.round(chars * modelRatio(run.model))
      const exact = !!(run.usage && typeof run.usage.outputTokens === 'number')
      const tokSec = tokens * 1000 / genMs

      // A call whose deltas all arrived in one or two bursts (short tool-call
      // arguments, cached replays) yields a mathematically "exact" but
      // meaningless rate: the denominator is a transport burst, not generation
      // time. Flag those so the UI shows tokens without a bogus speed.
      const reliable = genMs >= 500 || (tokens >= 40 && genMs >= 200)

      // calibrate the per-model chars→tokens ratio (EMA)
      if (run.usage && typeof run.usage.outputTokens === 'number' && chars >= CALIBRATE_MIN_CHARS) {
        const sample = run.usage.outputTokens / chars
        if (sample > 0.05 && sample < 20) {
          const prev = ratioByModel.get(run.model)
          const ratio = prev ? prev.ratio * 0.8 + sample * 0.2 : sample
          ratioByModel.set(run.model, { ratio, n: (prev ? prev.n : 0) + 1 })
        }
      }

      last = {
        sessionId: run.sessionId,
        model: run.model,
        provider: run.provider,
        purpose: run.purpose,
        tokens,
        chars,
        exact,
        reliable,
        genMs,
        tokSec: reliable ? Math.round(tokSec * 10) / 10 : null,
        rawTokSec: Math.round(tokSec * 10) / 10,
        finishKind: run.finishKind,
        finishedAt: now,
      }
      recent.unshift(last)
      if (recent.length > RECENT_KEEP) recent.pop()
    }

    function runningSnapshot() {
      if (!running) return null
      const now = Date.now()
      const chars = countedChars(running)
      if (!running.firstDeltaAt) {
        return { createdAt: running.startedAt, seq: now, waiting: true }
      }
      const since = running.firstDeltaAt
      const elapsedMs = Math.max(1, now - since)
      const ratio = modelRatio(running.model)
      const estTokens = Math.round(chars * ratio)

      // instant window rate from sampled points
      let windowTokSec = null
      const points = running.points
      points.push([now, chars])
      const cutoff = now - (WINDOW_MS + 200)
      while (points.length > 2 && points[0][0] < cutoff) points.shift()
      const span = now - points[0][0]
      if (span >= WINDOW_MIN_MS) {
        windowTokSec = ((chars - points[0][1]) * ratio) * 1000 / span
      }
      if (points.length > 600) points.splice(0, 300)

      return {
        createdAt: since,
        seq: now,
        waiting: false,
        sessionId: running.sessionId,
        model: running.model,
        provider: running.provider,
        purpose: running.purpose,
        chars,
        ratio: Math.round(ratio * 100) / 100,
        estTokens,
        genMs: elapsedMs,
        tokSec: Math.round((estTokens * 1000 / elapsedMs) * 10) / 10,
        windowTokSec: windowTokSec == null ? null : Math.round(windowTokSec * 10) / 10,
      }
    }

    function snapshot() {
      const r = runningSnapshot()
      return {
        running: r && r.waiting ? Object.assign({ model: running.model, sessionId: running.sessionId, purpose: running.purpose }, r) : r,
        last,
        recent,
        calibrated: [...ratioByModel.entries()].map(([m, v]) => ({ model: m, ratio: Math.round(v.ratio * 100) / 100, n: v.n })),
      }
    }

    // ── routes ──────────────────────────────────────────────────────────────
    function sendJson(res, obj) {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' })
      res.end(JSON.stringify(obj))
    }

    // Debug/inspection hook: another plugin or an operator tool can synchronously
    // pull the current snapshot without going through the HTTP layer (which the
    // desktop host gates behind its renderer token).
    ctx.on('speed-meter/snapshot', function (reply) {
      try {
        if (typeof reply === 'function') reply(snapshot())
      } catch (e) { /* ignore observer errors */ }
    })

    if (webServer && typeof webServer.register === 'function') {
      // Routes hang off ctx.effect so a fiber dispose (HMR reload, plugin
      // toggle) removes them; a duplicate registration then only happens when
      // the previous generation itself leaked the path, which we tolerate
      // instead of failing the whole plugin.
      const register = (route) => {
        try {
          const disposer = webServer.register(route)
          if (typeof disposer === 'function') return disposer
          if (disposer && typeof disposer.dispose === 'function') return () => disposer.dispose()
        } catch (e) {
          if (/duplicate/i.test(String((e && e.message) || e))) return () => {}
          throw e
        }
      }
      const mount = (route, label) => {
        if (typeof ctx.effect === 'function') ctx.effect(() => register(route), 'speed-meter: ' + label)
        else register(route)
      }

      mount({
        kind: 'exact',
        path: '/speed-meter/api',
        handler: async (req, res) => {
          try {
            sendJson(res, snapshot())
          } catch (e) {
            sendJson(res, { running: null, last: null, recent: [], calibrated: [], error: String(e) })
          }
        },
      }, 'api')

      mount({
        kind: 'exact',
        path: '/speed-meter/api/stream',
        handler: (req, res) => {
          res.writeHead(200, {
            'Content-Type': 'text/event-stream; charset=utf-8',
            'Cache-Control': 'no-cache',
            Connection: 'keep-alive',
            'X-Accel-Buffering': 'no',
          })
          res.write(':ok\n\n')
          let sent = ''
          const iv = setInterval(() => {
            try {
              const body = JSON.stringify(snapshot())
              if (body !== sent) {
                res.write(`data: ${body}\n\n`)
                sent = body
              } else {
                res.write(': tick\n\n')
              }
            } catch (e) {
              /* client gone mid-write; 'close' cleans up */
            }
          }, 400)
          if (iv.unref) iv.unref()
          req.on('close', () => clearInterval(iv))
        },
      }, 'api stream')
    }
  },
}
