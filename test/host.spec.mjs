// Host-half unit tests: exercise the llm/stream tap and the snapshot surface
// with no DSH runtime present.
import test from 'node:test'
import assert from 'node:assert/strict'
import plugin, { __reset, state } from '../lib/index.js'

// State lives at module scope (shared across fiber generations on purpose), so
// every test starts from a clean slate.
test.beforeEach(() => { __reset() })

function makeWebServer() {
  // Mirrors @deepseek-ai/dsh-host-webserver: a public exact Map, duplicate
  // registration throws, register() returns the removing disposer.
  const ws = {
    exact: new Map(),
    register(route) {
      if (ws.exact.has(route.path)) throw new Error(`webserver: duplicate exact route "${route.path}"`)
      ws.exact.set(route.path, route)
      return () => ws.exact.delete(route.path)
    },
  }
  return ws
}

function makeCtx(sharedWs) {
  const handlers = new Map()
  const ws = sharedWs || makeWebServer()
  const ctx = {
    on(name, fn) {
      handlers.set(name, fn)
      return () => { if (handlers.get(name) === fn) handlers.delete(name) }
    },
    get(name) { return name === 'webServer' ? ws : undefined },
    // cordis effect: run the factory and keep its disposer
    effect(fn) { return fn() },
    emit(name, ...args) {
      const fn = handlers.get(name)
      if (fn) fn(...args)
    },
  }
  return { ctx, handlers, routes: ws.exact }
}

function chunkStream(chunks, gapMs = 6) {
  return (async function* () {
    for (const c of chunks) {
      // simulate stream pacing so elapsed time is real
      await new Promise(r => setTimeout(r, gapMs))
      yield c
    }
  })()
}

function snapshotOf(handlers) {
  let snap = null
  handlers.get('speed-meter/snapshot')((s) => { snap = s })
  assert.ok(snap, 'snapshot handler answered')
  return snap
}

function snapshotOfSid(handlers, sessionId) {
  let snap = null
  handlers.get('speed-meter/snapshot')(sessionId, (s) => { snap = s })
  assert.ok(snap, 'session snapshot handler answered')
  return snap
}

test('registers the JSON and SSE routes', () => {
  const { ctx, routes } = makeCtx()
  plugin.apply(ctx)
  const paths = [...routes.keys()].sort()
  assert.deepEqual(paths, ['/speed-meter/api', '/speed-meter/api/stream'])
})

test('counts text/reasoning/tool-call deltas and finalizes with exact usage', async () => {
  const { ctx, handlers } = makeCtx()
  plugin.apply(ctx)
  const tap = handlers.get('llm/stream')
  assert.ok(tap, 'llm/stream tap installed')

  const stream = tap(
    { model: 'test-model', provider: 'test', sessionId: 's1' },
    () => chunkStream([
      { type: 'block-start', index: 0, blockType: 'reasoning' },
      { type: 'reasoning-delta', index: 0, text: 'x'.repeat(400) },
      { type: 'text-delta', index: 1, text: 'y'.repeat(300) },
      { type: 'text-delta', index: 1, text: 'y'.repeat(300) },
      { type: 'text-delta', index: 1, text: 'y'.repeat(300) },
      { type: 'text-delta', index: 1, text: 'y'.repeat(300) },
      { type: 'tool-call-delta', index: 2, id: 'c1', name: 'f', argumentsDelta: '{"a":1}' },
      { type: 'usage', usage: { outputTokens: 500, inputTokens: 10, cacheReadTokens: 0 } },
      { type: 'finish', reason: { kind: 'tool-calls' } },
    ], 80)
  )

  for await (const _ of stream) { /* drain */ }

  const snap = snapshotOf(handlers)
  assert.equal(snap.running, null)
  assert.equal(snap.last.tokens, 500)
  assert.equal(snap.last.exact, true)
  assert.equal(snap.last.chars, 400 + 1200 + 7)
  assert.equal(snap.last.reliable, true)
  assert.ok(snap.last.tokSec > 0)
  // ratio = 500 / 1607, EMA calibrated from the completed call (served rounded to 2dp)
  assert.equal(snap.calibrated.length, 1)
  assert.equal(snap.calibrated[0].ratio, Math.round((500 / 1607) * 100) / 100)
})

test('marks bursty short calls unreliable instead of reporting a fake rate', async () => {
  const { ctx, handlers } = makeCtx()
  plugin.apply(ctx)
  const tap = handlers.get('llm/stream')

  // all deltas delivered in one burst: a few ms of wall clock
  const stream = tap(
    { model: 'test-model', sessionId: 's2' },
    () => chunkStream([
      { type: 'text-delta', index: 0, text: 'z'.repeat(60) },
      { type: 'usage', usage: { outputTokens: 12 } },
      { type: 'finish', reason: { kind: 'stop' } },
    ])
  )
  for await (const _ of stream) { /* drain */ }

  const snap = snapshotOf(handlers)
  assert.equal(snap.last.reliable, false)
  assert.equal(snap.last.tokSec, null)
  assert.equal(snap.last.rawTokSec > 0, true)
})

test('exposes the running snapshot while the stream is open', async () => {
  const { ctx, handlers } = makeCtx()
  plugin.apply(ctx)
  const tap = handlers.get('llm/stream')

  const stream = tap(
    { model: 'test-model', sessionId: 's3' },
    () => chunkStream([
      { type: 'text-delta', index: 0, text: 'a'.repeat(100) },
      { type: 'text-delta', index: 0, text: 'b'.repeat(100) },
    ])
  )
  const it = stream[Symbol.asyncIterator]()

  await it.next() // take the first delta only
  let snap = snapshotOf(handlers)
  assert.ok(snap.running, 'running snapshot present')
  assert.equal(snap.running.chars, 100)
  assert.equal(snap.running.model, 'test-model')
  assert.equal(snap.running.windowTokSec == null || snap.running.windowTokSec > 0, true)

  for await (const _ of { [Symbol.asyncIterator]: () => it }) { /* drain rest */ }
  snap = snapshotOf(handlers)
  assert.equal(snap.running, null)
  assert.equal(snap.last.tokens, Math.round(200 * 3.6)) // no usage => estimate
})

test('survives a fiber generation swap: state is module scope, not closure scope', async () => {
  // generation 1: a call completes and leaves state behind
  const g1 = makeCtx()
  plugin.apply(g1.ctx)
  const tap1 = g1.handlers.get('llm/stream')
  const s1 = tap1(
    { model: 'gen-model', sessionId: 'sg' },
    () => chunkStream([
      { type: 'text-delta', index: 0, text: 'q'.repeat(200) },
      { type: 'text-delta', index: 0, text: 'q'.repeat(200) },
      { type: 'usage', usage: { outputTokens: 120 } },
      { type: 'finish', reason: { kind: 'stop' } },
    ], 60)
  )
  for await (const _ of s1) { /* drain */ }
  const before = snapshotOf(g1.handlers)
  assert.equal(before.last.tokens, 120)

  // generation 2 mounts against the SAME webserver instance: the old generation
  // still holds both paths, which must not throw and must end up owned by the
  // newest generation.
  const ws = g1.routes
  const g2ctx = {
    handlers: new Map(),
    on(name, fn) { this.handlers.set(name, fn) },
    get(name) { return name === 'webServer' ? ws : undefined },
    effect(fn) { return fn() },
  }
  plugin.apply(g2ctx)
  assert.deepEqual([...ws.keys()].sort(), ['/speed-meter/api', '/speed-meter/api/stream'], 'newest generation owns the routes')
  const after = snapshotOf(g2ctx.handlers)
  assert.equal(after.last.tokens, 120, 'state survived the generation swap')
  assert.equal(state().debug.generation, 2)
})

test('a fresh module instance (ESM cache cleared on reload) reads the same global state', async () => {
  const g1 = makeCtx()
  plugin.apply(g1.ctx)
  const tap = g1.handlers.get('llm/stream')
  const s1 = tap(
    { model: 'global-model', sessionId: 'sx' },
    () => chunkStream([
      { type: 'text-delta', index: 0, text: 'g'.repeat(300) },
      { type: 'text-delta', index: 0, text: 'g'.repeat(300) },
      { type: 'usage', usage: { outputTokens: 90 } },
      { type: 'finish', reason: { kind: 'stop' } },
    ], 60)
  )
  for await (const _ of s1) { /* drain */ }
  assert.equal(snapshotOf(g1.handlers).last.tokens, 90)

  // second, independent module instance — what a reload produces after the ESM
  // cache is cleared. This is the case that used to serve a frozen copy.
  const fresh = await import('../lib/index.js?reload-probe=1')
  const snap = fresh.snapshot()
  assert.equal(snap.last.tokens, 90, 'new module instance sees the shared state')
  assert.equal(fresh.state().debug.generation, 1, 'generation counter carried in the global state')
})

test('scopes the snapshot to one session so the dock never shows another window', async () => {
  const { ctx, handlers } = makeCtx()
  plugin.apply(ctx)
  const tap = handlers.get('llm/stream')

  async function call(sessionId, tokens, chars) {
    const stream = tap(
      { model: 'm', sessionId },
      () => chunkStream([
        { type: 'text-delta', index: 0, text: 'a'.repeat(chars) },
        { type: 'usage', usage: { outputTokens: tokens } },
        { type: 'finish', reason: { kind: 'stop' } },
      ], 60)
    )
    for await (const _ of stream) { /* drain */ }
  }

  await call('A', 111, 400)
  await call('B', 222, 800)

  const viewA = snapshotOfSid(handlers, 'A')
  const viewB = snapshotOfSid(handlers, 'B')
  assert.equal(viewA.last.sessionId, 'A')
  assert.equal(viewA.last.tokens, 111)
  assert.deepEqual(viewA.recent.map(r => r.tokens), [111])
  assert.equal(viewB.last.sessionId, 'B')
  assert.equal(viewB.last.tokens, 222)
  assert.deepEqual(viewB.recent.map(r => r.tokens), [222])

  // no session => process-wide latest (operator view)
  const globalSnap = snapshotOf(handlers)
  assert.equal(globalSnap.last.sessionId, 'B')
  assert.deepEqual(globalSnap.recent.map(r => r.tokens).sort((x, y) => y - x), [222, 111])
})

test('a running call in another session is reported as elsewhere, not as this session\'s rate', async () => {
  const { ctx, handlers } = makeCtx()
  plugin.apply(ctx)
  const tap = handlers.get('llm/stream')

  const stream = tap(
    { model: 'm', sessionId: 'OTHER' },
    () => chunkStream([{ type: 'text-delta', index: 0, text: 'x'.repeat(300) }], 40)
  )
  const it = stream[Symbol.asyncIterator]()
  await it.next() // stream is live in OTHER

  const view = snapshotOfSid(handlers, 'MINE')
  assert.equal(view.running, null, 'no foreign rate leaks into this session')
  assert.equal(view.runningElsewhere.sessionId, 'OTHER')

  for await (const _ of { [Symbol.asyncIterator]: () => it }) { /* drain */ }
})

test('normalizes a legacy global state left by an older build (no sessions field)', async () => {
  // Simulate the 0.1.1 singleton surviving a hot reload into 0.1.2+ code.
  const legacyKey = Symbol.for('dsh-speed-meter/state-v1')
  globalThis[legacyKey] = {
    running: null, last: null, recent: [], ratioByModel: new Map(),
    debug: { apiHits: 1, sseHits: 0, sseActive: 0, lastHitAt: 0, lastPushAt: 0, generation: 3, taps: 9 },
  } // note: no `sessions`

  const { ctx, handlers } = makeCtx()
  plugin.apply(ctx)
  const tap = handlers.get('llm/stream')
  const stream = tap(
    { model: 'legacy-model', sessionId: 'L1' },
    () => chunkStream([
      { type: 'text-delta', index: 0, text: 'l'.repeat(300) },
      { type: 'usage', usage: { outputTokens: 77 } },
      { type: 'finish', reason: { kind: 'stop' } },
    ], 60)
  )
  // must not throw out of the stream unwind path
  for await (const _ of stream) { /* drain */ }

  const view = snapshotOfSid(handlers, 'L1')
  assert.equal(view.last.tokens, 77)
  assert.equal(view.last.sessionId, 'L1')
})

test('a throwing per-session store can never fail the model turn', async () => {
  const legacyKey = Symbol.for('dsh-speed-meter/state-v1')
  class ThrowingMap extends Map { get() { throw new Error('boom') } }
  globalThis[legacyKey] = {
    running: null, last: null, recent: [], sessions: new ThrowingMap(), ratioByModel: new Map(),
    debug: { apiHits: 0, sseHits: 0, sseActive: 0, lastHitAt: 0, lastPushAt: 0, generation: 0, taps: 0 },
  }

  const { ctx, handlers } = makeCtx()
  plugin.apply(ctx)
  const tap = handlers.get('llm/stream')
  const stream = tap(
    { model: 'm', sessionId: 'S' },
    () => chunkStream([
      { type: 'text-delta', index: 0, text: 'z'.repeat(100) },
      { type: 'usage', usage: { outputTokens: 10 } },
      { type: 'finish', reason: { kind: 'stop' } },
    ], 20)
  )
  const seen = []
  for await (const c of stream) seen.push(c.type) // must complete without throwing
  assert.deepEqual(seen, ['text-delta', 'usage', 'finish'])
  assert.match(state().debug.lastError || '', /boom/)
})

test('a new generation retires the previous llm/stream tap (no double counting)', async () => {
  const ws = makeWebServer()
  const g1 = makeCtx(ws)
  plugin.apply(g1.ctx)
  const tap1 = g1.handlers.get('llm/stream')
  assert.ok(tap1)

  const g2 = makeCtx(ws)
  plugin.apply(g2.ctx) // same process, same webserver
  const tap2 = g2.handlers.get('llm/stream')
  assert.ok(tap2)
  assert.notEqual(tap1, tap2, 'new generation installed its own tap')
  assert.equal(g1.handlers.has('llm/stream'), false, 'previous tap was disposed')

  // one call observed only by the surviving tap => exactly one record
  const stream = tap2(
    { model: 'm', sessionId: 'D1' },
    () => chunkStream([
      { type: 'text-delta', index: 0, text: 'x'.repeat(400) },
      { type: 'usage', usage: { outputTokens: 88 } },
      { type: 'finish', reason: { kind: 'stop' } },
    ], 60)
  )
  for await (const _ of stream) { /* drain */ }
  const view = snapshotOfSid(g2.handlers, 'D1')
  assert.equal(view.recent.length, 1)
  assert.equal(view.last.tokens, 88)
})

test('collapses the duplicate row produced when two taps observe one call', async () => {
  const { ctx, handlers } = makeCtx()
  plugin.apply(ctx)
  const tap = handlers.get('llm/stream')
  // a run whose finalize is executed twice (what a stale second tap used to do)
  const mk = () => tap({ model: 'm', sessionId: 'DD' }, () => chunkStream([
    { type: 'text-delta', index: 0, text: 'y'.repeat(500) },
    { type: 'usage', usage: { outputTokens: 66 } },
    { type: 'finish', reason: { kind: 'stop' } },
  ], 60))
  for await (const _ of mk()) { /* drain */ }
  for await (const _ of mk()) { /* drain */ }
  const view = snapshotOfSid(handlers, 'DD')
  assert.equal(view.recent.length, 1, 'identical totals collapse into one row')
})

test('a finished turn reports its average speed and cumulative usage', async () => {
  const { ctx, handlers } = makeCtx()
  plugin.apply(ctx)
  const tap = handlers.get('llm/stream')

  async function call(tokens, chars, gapMs) {
    const stream = tap(
      { model: 'm', sessionId: 'TURN' },
      () => chunkStream([
        { type: 'text-delta', index: 0, text: 'a'.repeat(chars) },
        { type: 'usage', usage: { outputTokens: tokens } },
        { type: 'finish', reason: { kind: 'stop' } },
      ], gapMs)
    )
    for await (const _ of stream) { /* drain */ }
  }

  // turn starts (the dock must keep quiet until the turn ends)
  ctx.emit('agent/status', { agent: { session: { id: 'session-TURN' } }, status: 'running' })
  await call(120, 400, 60)
  await call(80, 300, 60)
  const during = snapshotOfSid(handlers, 'TURN').turn
  assert.equal(during.active, true)
  assert.equal(during.calls, 2)
  assert.equal(during.tokens, 200)

  // turn ends -> the pill switches to this turn's aggregate
  ctx.emit('agent/status', { agent: { session: { id: 'session-TURN' } }, status: 'idle' })
  const view = snapshotOfSid(handlers, 'TURN')
  const t = view.turn
  assert.equal(t.active, false)
  assert.equal(t.calls, 2)
  assert.equal(t.tokens, 200)
  assert.equal(t.chars, 700)
  assert.ok(t.genMs > 0)
  assert.equal(t.tokSec, Math.round((200 * 1000 / t.genMs) * 10) / 10)
  assert.equal(t.exact, true)
  assert.deepEqual(t.models, ['m'])
  // the last single call is still available for the tooltip
  assert.equal(view.last.tokens, 80)
})

test('a call with no running transition still opens an implicit turn', async () => {
  const { ctx, handlers } = makeCtx()
  plugin.apply(ctx)
  const tap = handlers.get('llm/stream')
  const stream = tap(
    { model: 'm', sessionId: 'IMP' },
    () => chunkStream([
      { type: 'text-delta', index: 0, text: 'b'.repeat(200) },
      { type: 'usage', usage: { outputTokens: 50 } },
      { type: 'finish', reason: { kind: 'stop' } },
    ], 60)
  )
  for await (const _ of stream) { /* drain */ }
  const t = snapshotOfSid(handlers, 'IMP').turn
  assert.ok(t, 'turn exists even without a status event')
  assert.equal(t.calls, 1)
  assert.equal(t.tokens, 50)
  assert.equal(t.active, true)
})

// ── remote bridge (client mode → remote Host snapshot) ─────────────────────

function remoteCtx(remoteClient) {
  const { ctx, handlers, routes } = makeCtx()
  const original = ctx.get
  ctx.get = (name) => (name === 'dshRemoteClient' ? remoteClient : original(name))
  return { ctx, handlers, routes }
}

function callRoute(route, url = '/speed-meter/api') {
  return new Promise((resolve) => {
    const res = {
      status: 0, body: '', chunks: [],
      writeHead(c) { this.status = c; return this },
      end(b) { this.chunks.push(b == null ? '' : String(b)); this.body = this.chunks.join(''); resolve(JSON.parse(this.body)) },
      write(b) { this.chunks.push(String(b)); return true },
      on() { return this }, once() { return this }, setHeader() {}, getHeader() {}, removeHeader() {},
    }
    route.handler({ url, method: 'GET', headers: {}, on() { return this } }, res)
  })
}

test('client mode proxies the remote Host snapshot when configured', async () => {
  const remoteClient = {
    status: () => ({ mode: 'remote', connected: true }),
    preview: { open: async (port) => ({ url: 'http://127.0.0.1:4567/' }) },
  }
  const { ctx, routes } = remoteCtx(remoteClient)
  plugin.apply(ctx, { remote: { enabled: true, webPort: 57110 } })

  const realFetch = globalThis.fetch
  const calls = []
  globalThis.fetch = async (url) => {
    calls.push(String(url))
    return {
      ok: true, status: 200,
      json: async () => ({ running: null, last: { sessionId: 'R1', tokens: 999 }, recent: [], turn: null, calibrated: [] }),
    }
  }
  try {
    const body = await callRoute(routes.get('/speed-meter/api'), '/speed-meter/api?session=R1')
    assert.equal(body.source, 'remote')
    assert.equal(body.last.sessionId, 'R1')
    assert.equal(body.last.tokens, 999)
    assert.ok(calls[0].startsWith('http://127.0.0.1:4567/speed-meter/api'), 'uses the preview origin: ' + calls[0])
    assert.ok(calls[0].includes('session=R1'))
    assert.equal(body.remote.port, 57110)
  } finally { globalThis.fetch = realFetch }
})

test('without configuration the pill keeps local data', async () => {
  const { ctx, routes } = remoteCtx({ status: () => ({ mode: 'local', connected: false }), preview: { open: async () => ({ url: 'x' }) } })
  plugin.apply(ctx) // no remote config
  const body = await callRoute(routes.get('/speed-meter/api'))
  assert.equal(body.source, 'local')
})

test('a failing preview falls back to local data and records the error', async () => {
  const remoteClient = {
    status: () => ({ mode: 'remote', connected: true }),
    preview: { open: async () => { throw new Error('PORT_NOT_ALLOWED') } },
  }
  const { ctx, routes } = remoteCtx(remoteClient)
  plugin.apply(ctx, { remote: { enabled: true, webPort: 57110 } })
  const body = await callRoute(routes.get('/speed-meter/api'))
  assert.equal(body.source, 'local')
  assert.match(body.remote.lastError, /PORT_NOT_ALLOWED/)
})
