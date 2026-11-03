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
 * State lives on a PROCESS-GLOBAL singleton, not in module scope and not in the
 * apply()/fiber closure. Why: a Cordis hot reload clears the ESM module cache
 * and mounts a new fiber, but the HTTP route table may keep the handler
 * registered by the previous generation (Cordis builds the replacement fiber
 * before retiring the old one, so a duplicate path is expected and tolerated
 * here). A closure- or module-scoped state would then be read by a retired
 * handler copy while the live fiber writes a different copy — the gauge freezes
 * at the reload instant. With one process-global state every generation, and
 * every surviving route, observes and serves the same live data.
 */

/** Sliding window (ms) used for the "instant" rate shown while streaming. */
const WINDOW_MS = 1200
/** Minimum window span (ms) before the instant rate is trusted. */
const WINDOW_MIN_MS = 250
/** Min chars before a finished call may calibrate the model ratio. */
const CALIBRATE_MIN_CHARS = 200
/** Max recent-call rows served to the client half. */
const RECENT_KEEP = 12

const STATE_KEY = Symbol.for('dsh-speed-meter/state-v1')

/** Process-global live state shared by every fiber generation. */
function state() {
  let st = globalThis[STATE_KEY]
  if (!st) {
    st = {
      running: null,
      last: null,
      recent: [],
      ratioByModel: new Map(),
      debug: { apiHits: 0, sseHits: 0, sseActive: 0, lastHitAt: 0, lastPushAt: 0, generation: 0, taps: 0 },
    }
    Object.defineProperty(globalThis, STATE_KEY, { value: st, configurable: true })
  }
  return st
}

function safeGet(obj, key) {
  try {
    return obj ? obj[key] : undefined
  } catch (e) {
    return undefined
  }
}

function modelRatio(st, model) {
  const r = st.ratioByModel.get(model)
  return r ? r.ratio : 3.6
}

function countedChars(run) {
  return run.textChars + run.reasoningChars + run.toolChars
}

// ── llm/stream tap ──────────────────────────────────────────────────────────
function makeTap() {
  return function handleLlmStream(options, next) {
    const source = next()
    if (!options || typeof options.model !== 'string') return source
    const st = state()
    st.debug.taps++

    const run = {
      sessionId: options.sessionId ? String(options.sessionId) : '',
      model: options.model,
      provider: String(options.provider || ''),
      purpose: options.purpose ? String(options.purpose) : '',
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
    st.running = run

    function noteDelta(now) {
      if (!run.firstDeltaAt) run.firstDeltaAt = now
      run.lastDeltaAt = now
    }

    async function* observe() {
      try {
        for await (const chunk of source) {
          if (chunk && typeof chunk === 'object') {
            const now = Date.now()
            if (chunk.type === 'text-delta' && chunk.text) {
              noteDelta(now)
              run.textChars += chunk.text.length
            } else if (chunk.type === 'reasoning-delta' && chunk.text) {
              noteDelta(now)
              run.reasoningChars += chunk.text.length
            } else if (chunk.type === 'tool-call-delta' && chunk.argumentsDelta) {
              noteDelta(now)
              run.toolChars += chunk.argumentsDelta.length
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
        if (st.running === run) st.running = null
      }
    }
    return observe()
  }
}

function finalize(run) {
  const st = state()
  const chars = countedChars(run)
  const now = Date.now()
  const endAt = run.lastDeltaAt || now
  const genMs = run.firstDeltaAt ? Math.max(1, endAt - run.firstDeltaAt) : Math.max(1, now - run.startedAt)

  // exact token count when the adapter reported usage; otherwise estimate
  const exact = !!(run.usage && typeof run.usage.outputTokens === 'number')
  const tokens = exact ? run.usage.outputTokens : Math.round(chars * modelRatio(st, run.model))
  const tokSec = tokens * 1000 / genMs

  // A call whose deltas all arrived in one or two bursts (short tool-call
  // arguments, cached replays) yields a mathematically "exact" but meaningless
  // rate: the denominator is a transport burst, not generation time. Flag those
  // so the UI shows tokens without a bogus speed.
  const reliable = genMs >= 500 || (tokens >= 40 && genMs >= 200)

  // calibrate the per-model chars→tokens ratio (EMA)
  if (exact && chars >= CALIBRATE_MIN_CHARS) {
    const sample = run.usage.outputTokens / chars
    if (sample > 0.05 && sample < 20) {
      const prev = st.ratioByModel.get(run.model)
      const ratio = prev ? prev.ratio * 0.8 + sample * 0.2 : sample
      st.ratioByModel.set(run.model, { ratio, n: (prev ? prev.n : 0) + 1 })
    }
  }

  st.last = {
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
  st.recent.unshift(st.last)
  if (st.recent.length > RECENT_KEEP) st.recent.pop()
}

function runningSnapshot(st) {
  const run = st.running
  if (!run) return null
  const now = Date.now()
  const chars = countedChars(run)
  if (!run.firstDeltaAt) {
    return { model: run.model, sessionId: run.sessionId, purpose: run.purpose, createdAt: run.startedAt, seq: now, waiting: true }
  }
  const since = run.firstDeltaAt
  const elapsedMs = Math.max(1, now - since)
  const ratio = modelRatio(st, run.model)
  const estTokens = Math.round(chars * ratio)

  // instant window rate from sampled points
  let windowTokSec = null
  const points = run.points
  points.push([now, chars])
  const cutoff = now - (WINDOW_MS + 200)
  while (points.length > 2 && points[0][0] < cutoff) points.shift()
  const span = now - points[0][0]
  if (span >= WINDOW_MIN_MS) windowTokSec = ((chars - points[0][1]) * ratio) * 1000 / span
  if (points.length > 600) points.splice(0, 300)

  return {
    createdAt: since,
    seq: now,
    waiting: false,
    sessionId: run.sessionId,
    model: run.model,
    provider: run.provider,
    purpose: run.purpose,
    chars,
    ratio: Math.round(ratio * 100) / 100,
    estTokens,
    genMs: elapsedMs,
    tokSec: Math.round((estTokens * 1000 / elapsedMs) * 10) / 10,
    windowTokSec: windowTokSec == null ? null : Math.round(windowTokSec * 10) / 10,
  }
}

function snapshot() {
  const st = state()
  return {
    running: runningSnapshot(st),
    last: st.last,
    recent: st.recent,
    calibrated: [...st.ratioByModel.entries()].map(([m, v]) => ({ model: m, ratio: Math.round(v.ratio * 100) / 100, n: v.n })),
    debug: st.debug,
  }
}

function sendJson(res, obj) {
  res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' })
  res.end(JSON.stringify(obj))
}

function apiHandler(req, res) {
  const dbg = state().debug
  dbg.apiHits++
  dbg.lastHitAt = Date.now()
  try {
    sendJson(res, snapshot())
  } catch (e) {
    sendJson(res, { running: null, last: null, recent: [], calibrated: [], error: String(e) })
  }
}

function sseHandler(req, res) {
  const dbg = state().debug
  dbg.sseHits++
  dbg.sseActive++
  dbg.lastHitAt = Date.now()
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
        dbg.lastPushAt = Date.now()
      } else {
        res.write(': tick\n\n')
      }
    } catch (e) {
      /* client gone mid-write; 'close' cleans up */
    }
  }, 400)
  if (iv.unref) iv.unref()
  req.on('close', () => {
    clearInterval(iv)
    dbg.sseActive = Math.max(0, dbg.sseActive - 1)
  })
}

export default {
  apply(ctx) {
    const st = state()
    st.debug.generation++

    let webServer
    try { webServer = typeof ctx.get === 'function' ? ctx.get('webServer') : undefined } catch (e) { webServer = undefined }
    if (webServer === undefined) webServer = safeGet(ctx, 'webServer')

    ctx.on('llm/stream', makeTap())

    // Debug/inspection hook: another plugin or an operator tool can synchronously
    // pull the current snapshot without going through the HTTP layer.
    ctx.on('speed-meter/snapshot', function (reply) {
      try {
        if (typeof reply === 'function') reply(snapshot())
      } catch (e) { /* ignore observer errors */ }
    })

    if (webServer && typeof webServer.register === 'function') {
      // Routes hang off ctx.effect so a fiber dispose (HMR reload, plugin
      // toggle) removes them. If a previous generation still holds the path the
      // duplicate is tolerated: it serves the same process-global state anyway.
      const register = (route) => {
        const attempt = () => {
          const disposer = webServer.register(route)
          if (typeof disposer === 'function') return disposer
          if (disposer && typeof disposer.dispose === 'function') return () => disposer.dispose()
        }
        try {
          return attempt()
        } catch (e) {
          if (!/duplicate/i.test(String((e && e.message) || e))) throw e
          // Cordis mounts the replacement fiber BEFORE retiring the old one, so
          // our own path can still be held by the previous generation. Claim it
          // back: the newest generation must own the route (both serve the same
          // process-global state, but only the newest one is guaranteed current).
          try {
            const table = webServer.exact
            if (table && typeof table.delete === 'function') table.delete(route.path)
            return attempt()
          } catch (e2) {
            if (/duplicate/i.test(String((e2 && e2.message) || e2))) return () => {}
            throw e2
          }
        }
      }
      const mount = (route, label) => {
        if (typeof ctx.effect === 'function') ctx.effect(() => register(route), 'speed-meter: ' + label)
        else register(route)
      }
      mount({ kind: 'exact', path: '/speed-meter/api', handler: apiHandler }, 'api')
      mount({ kind: 'exact', path: '/speed-meter/api/stream', handler: sseHandler }, 'api stream')
    }
  },
}

/** Reset the process-global state (unit tests only). */
function __reset() {
  try { delete globalThis[STATE_KEY] } catch (e) { globalThis[STATE_KEY] = undefined }
}

export { snapshot, finalize, makeTap, state, __reset }
