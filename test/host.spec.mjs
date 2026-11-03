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
    on(name, fn) { handlers.set(name, fn) },
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
