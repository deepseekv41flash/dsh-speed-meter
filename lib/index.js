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

/** Import the Remote bridge helper (client-mode proxy to the remote Host). */
import { RemoteBridge } from './remote.js'

/** Sliding window (ms) used for the "instant" rate shown while streaming. */
const WINDOW_MS = 1200
/** Minimum window span (ms) before the instant rate is trusted. */
const WINDOW_MIN_MS = 250
/** Min chars before a finished call may calibrate the model ratio. */
const CALIBRATE_MIN_CHARS = 200
/** Max recent-call rows served to the client half. */
const RECENT_KEEP = 12

const STATE_KEY = Symbol.for('dsh-speed-meter/state-v1')

/**
 * Process-global live state shared by every fiber generation.
 *
 * The singleton outlives hot reloads, so a NEWER build may find an object
 * created by an OLDER one. Every field is therefore normalized on access: a
 * version that adds a field must never assume it exists on a state object left
 * behind by the previous generation (that exact mismatch once crashed turns
 * with `Cannot read properties of undefined (reading 'get')`).
 */
function state() {
  let st = globalThis[STATE_KEY]
  if (!st || typeof st !== 'object') {
    st = {}
    Object.defineProperty(globalThis, STATE_KEY, { value: st, configurable: true })
  }
  if (!(st.sessions instanceof Map)) st.sessions = new Map()
  if (!(st.ratioByModel instanceof Map)) st.ratioByModel = new Map()
  if (!Array.isArray(st.recent)) st.recent = []
  if (st.running === undefined) st.running = null
  if (st.last === undefined) st.last = null
  if (!st.debug || typeof st.debug !== 'object') st.debug = {}
  const dbg = st.debug
  if (typeof dbg.apiHits !== 'number') dbg.apiHits = 0
  if (typeof dbg.sseHits !== 'number') dbg.sseHits = 0
  if (typeof dbg.sseActive !== 'number') dbg.sseActive = 0
  if (typeof dbg.lastHitAt !== 'number') dbg.lastHitAt = 0
  if (typeof dbg.lastPushAt !== 'number') dbg.lastPushAt = 0
  if (typeof dbg.generation !== 'number') dbg.generation = 0
  if (typeof dbg.taps !== 'number') dbg.taps = 0
  return st
}

/** Keep at most this many per-session entries (LRU by touch time). */
const MAX_SESSIONS = 48

/** Session ids appear both bare and `session-`-prefixed; key on the bare form. */
function normId(sessionId) {
  return String(sessionId || '').replace(/^session-/i, '')
}

function sessionState(st, sessionId) {
  const key = normId(sessionId)
  let s = st.sessions.get(key)
  if (!s) {
    s = { last: null, recent: [], turn: null, touchedAt: 0 }
    st.sessions.set(key, s)
    if (st.sessions.size > MAX_SESSIONS) {
      let oldestKey = null
      let oldest = Infinity
      for (const [k, value] of st.sessions) {
        if (value.touchedAt < oldest) { oldest = value.touchedAt; oldestKey = k }
      }
      if (oldestKey !== null) st.sessions.delete(oldestKey)
    }
  }
  if (!Array.isArray(s.recent)) s.recent = []
  if (s.turn === undefined) s.turn = null
  s.touchedAt = Date.now()
  return s
}

// ── turn bookkeeping ────────────────────────────────────────────────────────
// A "turn" is one user message → the assistant's whole reply, which may span
// several model calls (tool rounds, retries). The dock reports this turn's
// average output speed once it ends, instead of only the last call's numbers.

function blankTurn(now) {
  return {
    startedAt: now,
    endedAt: null,
    calls: 0,
    tokens: 0,
    chars: 0,
    genMs: 0,
    exactAll: true,
    models: [],
  }
}

function beginTurn(st, sessionId) {
  if (!sessionId) return null
  const s = sessionState(st, sessionId)
  if (s.turn && s.turn.endedAt == null) return s.turn // already open
  s.turn = blankTurn(Date.now())
  return s.turn
}

function endTurn(st, sessionId) {
  const s = sessionState(st, sessionId)
  if (!s.turn || s.turn.endedAt != null) return s.turn
  s.turn.endedAt = Date.now()
  return s.turn
}

/** Add one finished model call to the session's open turn (opening one if needed). */
function accumulateTurn(st, sessionId, record) {
  if (!sessionId) return
  const s = sessionState(st, sessionId)
  if (!s.turn || s.turn.endedAt != null) {
    // a call without a preceding running transition (plugin loaded mid-turn,
    // title/summary call, or a status event we missed) still belongs to a turn
    s.turn = blankTurn(record.startedAt || record.finishedAt)
  }
  const t = s.turn
  t.calls++
  t.tokens += record.tokens
  t.chars += record.chars
  t.genMs += record.genMs
  if (!record.exact) t.exactAll = false
  if (record.model && t.models.indexOf(record.model) < 0) t.models.push(record.model)
}

/** Client-facing turn view (rates computed here so the client stays dumb). */
function turnView(turn) {
  if (!turn || !turn.calls) return null
  const wallMs = Math.max(1, (turn.endedAt || Date.now()) - turn.startedAt)
  const genMs = Math.max(1, turn.genMs)
  return {
    active: turn.endedAt == null,
    startedAt: turn.startedAt,
    endedAt: turn.endedAt,
    calls: turn.calls,
    tokens: turn.tokens,
    chars: turn.chars,
    genMs: turn.genMs,
    wallMs,
    exact: turn.exactAll,
    models: turn.models,
    tokSec: Math.round((turn.tokens * 1000 / genMs) * 10) / 10,
    wallTokSec: Math.round((turn.tokens * 1000 / wallMs) * 10) / 10,
  }
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
        // A gauge must never break the turn it observes: this finally runs in
        // the model stream's unwind path, so any throw here would surface as a
        // failed run (that is how the 0.1.2 state-shape bug crashed turns).
        try {
          finalize(run)
        } catch (e) {
          try {
            const dbg = state().debug
            dbg.lastError = String((e && e.message) || e)
          } catch (e2) { /* diagnostics only */ }
        }
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
    startedAt: run.startedAt,
    finishedAt: now,
  }
  pushRecord(st.recent, st.last)

  // per-session view (the dock shows only the session it is mounted in)
  if (run.sessionId) {
    const s = sessionState(st, run.sessionId)
    s.last = st.last
    const isNew = pushRecord(s.recent, st.last)
    // turn aggregation: only when the record is genuinely new, so a duplicate
    // finalize from a straggler tap cannot inflate the turn totals
    if (isNew) accumulateTurn(st, run.sessionId, st.last)
  }
}

/**
 * Append one finished call, collapsing the duplicate that appears when two tap
 * generations observe the SAME call (HMR keeps the retired listener alive for a
 * moment). Identical totals from the same session+model are the same call.
 *
 * @returns true when a new row was appended, false when a duplicate collapsed.
 */
function pushRecord(list, record) {
  const head = list[0]
  const same = head
    && head.sessionId === record.sessionId
    && head.model === record.model
    && head.purpose === record.purpose
    && head.tokens === record.tokens
    && head.chars === record.chars
    && head.genMs === record.genMs
  if (same) {
    head.finishedAt = record.finishedAt
    head.exact = record.exact
    head.reliable = record.reliable
    return false
  }
  list.unshift(record)
  if (list.length > RECENT_KEEP) list.pop()
  return true
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

/**
 * Snapshot for one viewer.
 *
 * @param sessionId - the session the requesting dock belongs to. When given,
 *   `running` is reported only for a call in that session, and `last`/`recent`
 *   come from that session's own history — never another window's numbers.
 *   Omitted (operator/debug views) => process-wide latest.
 */
function snapshot(sessionId) {
  const st = state()
  const sid = normId(typeof sessionId === 'string' ? sessionId : '')
  let running = null
  let last = null
  let recent = []
  let runningElsewhere = null
  let turn = null

  if (sid) {
    if (st.running && normId(st.running.sessionId) === sid) running = runningSnapshot(st)
    const s = st.sessions.get(sid)
    if (s) {
      last = s.last
      recent = s.recent
      turn = turnView(s.turn)
      s.touchedAt = Date.now()
    }
    if (st.running && normId(st.running.sessionId) !== sid) {
      runningElsewhere = { sessionId: st.running.sessionId, model: st.running.model }
    }
  } else {
    running = runningSnapshot(st)
    last = st.last
    recent = st.recent
    const s = st.last ? st.sessions.get(normId(st.last.sessionId)) : null
    turn = s ? turnView(s.turn) : null
  }

  return {
    running,
    last,
    recent,
    turn,
    runningElsewhere,
    calibrated: [...st.ratioByModel.entries()].map(([m, v]) => ({ model: m, ratio: Math.round(v.ratio * 100) / 100, n: v.n })),
    debug: st.debug,
  }
}

/** `?session=<id>` from a plugin request URL. */
function sessionFromUrl(url) {
  try {
    const q = String(url || '').indexOf('?')
    if (q < 0) return ''
    const search = new URLSearchParams(String(url).slice(q + 1))
    return search.get('session') || ''
  } catch (e) {
    return ''
  }
}

function sendJson(res, obj) {
  res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' })
  res.end(JSON.stringify(obj))
}

/**
 * One snapshot for a viewer, preferring the remote Host when this Harness is
 * connected to another machine in Remote client mode.
 *
 * @param bridge - the RemoteBridge created by apply() (may be undefined).
 * @param sessionId - the session id the dock asked about.
 * @returns a snapshot JSON with a `source` marker ('remote' | 'local').
 */
async function resolveSnapshot(bridge, sessionId) {
  if (bridge && bridge.enabled) {
    try {
      const remote = await bridge.fetchSnapshot(sessionId)
      remote.source = 'remote'
      remote.remote = bridge.debug
      return remote
    } catch (e) {
      bridge.debug.ok = false
      bridge.debug.lastError = String((e && e.message) || e)
      bridge.reset()
    }
  }
  const local = snapshot(sessionId)
  local.source = 'local'
  if (bridge) local.remote = bridge.debug
  return local
}

function apiHandlerFor(bridge) {
  return async function apiHandler(req, res) {
    const dbg = state().debug
    dbg.apiHits++
    dbg.lastHitAt = Date.now()
    try {
      sendJson(res, await resolveSnapshot(bridge, sessionFromUrl(req && req.url)))
    } catch (e) {
      sendJson(res, { running: null, last: null, recent: [], calibrated: [], error: String(e) })
    }
  }
}

function sseHandlerFor(bridge) {
  return function sseHandler(req, res) {
    const dbg = state().debug
    dbg.sseHits++
    dbg.sseActive++
    dbg.lastHitAt = Date.now()
    const sessionId = sessionFromUrl(req && req.url)
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    })
    res.write(':ok\n\n')
    let sent = ''
    let inFlight = false
    const push = async () => {
      if (inFlight) return
      inFlight = true
      try {
        const body = JSON.stringify(await resolveSnapshot(bridge, sessionId))
        if (body !== sent) {
          res.write(`data: ${body}\n\n`)
          sent = body
          dbg.lastPushAt = Date.now()
        } else {
          res.write(': tick\n\n')
        }
      } catch (e) {
        /* client gone mid-write; 'close' cleans up */
      } finally {
        inFlight = false
      }
    }
    const iv = setInterval(push, bridge && bridge.enabled ? Math.max(400, bridge.pollMs) : 400)
    void push()
    req.on('close', () => {
      clearInterval(iv)
      dbg.sseActive = Math.max(0, dbg.sseActive - 1)
    })
  }
}

export default {
  apply(ctx, config) {
    const st = state()
    st.debug.generation++
    // Remote client-mode bridge: when this Harness is a Remote client of another
    // Host, serve that Host's speed-meter snapshot instead of the local one.
    const bridge = new RemoteBridge(ctx, (config && config.remote) || {})
    st.remoteBridge = bridge

    let webServer
    try { webServer = typeof ctx.get === 'function' ? ctx.get('webServer') : undefined } catch (e) { webServer = undefined }
    if (webServer === undefined) webServer = safeGet(ctx, 'webServer')

    // Install the stream tap and retire the previous generation's tap: during
    // an HMR reload both listeners can be alive for a moment, which double-counts
    // a call and leaves duplicate rows in `recent`.
    const disposeTap = ctx.on('llm/stream', makeTap())
    if (typeof disposeTap === 'function') {
      const previous = st.disposeTap
      if (typeof previous === 'function' && previous !== disposeTap) {
        try { previous() } catch (e) { /* already gone */ }
      }
      st.disposeTap = disposeTap
    } else {
      st.disposeTap = null
    }

    // Turn boundaries: the agent reports idle ⇄ running. A turn opens when the
    // agent starts running and closes when it goes idle — that is the unit the
    // dock reports ("this turn's average speed and usage") once it ends.
    const disposeStatus = ctx.on('agent/status', function (payload) {
      try {
        const agent = payload && payload.agent
        const sid = agent && agent.session ? agent.session.id : ''
        if (!sid) return
        const s2 = state()
        if (payload.status === 'running') beginTurn(s2, sid)
        else if (payload.status === 'idle') endTurn(s2, sid)
      } catch (e) { /* never break the agent */ }
    })
    if (typeof disposeStatus === 'function') {
      const previous = st.disposeStatus
      if (typeof previous === 'function' && previous !== disposeStatus) {
        try { previous() } catch (e) { /* already gone */ }
      }
      st.disposeStatus = disposeStatus
    } else {
      st.disposeStatus = null
    }

    // Debug/inspection hook: another plugin or an operator tool can synchronously
    // pull the snapshot. `(reply)` => process-wide; `(sessionId, reply)` => the
    // same session-scoped view the dock sees.
    ctx.on('speed-meter/snapshot', function (a, b) {
      try {
        const cb = typeof b === 'function' ? b : (typeof a === 'function' ? a : null)
        const sid = typeof a === 'string' ? a : ''
        if (cb) cb(snapshot(sid))
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
      mount({ kind: 'exact', path: '/speed-meter/api', handler: apiHandlerFor(bridge) }, 'api')
      mount({ kind: 'exact', path: '/speed-meter/api/stream', handler: sseHandlerFor(bridge) }, 'api stream')
    }
  },
}

/** Reset the process-global state (unit tests only). */
function __reset() {
  try { delete globalThis[STATE_KEY] } catch (e) { globalThis[STATE_KEY] = undefined }
}

export { snapshot, finalize, makeTap, state, __reset }
