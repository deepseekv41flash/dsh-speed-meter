// Host-half unit tests: exercise the llm/stream tap and the snapshot surface
// with no DSH runtime present.
import test from 'node:test'
import assert from 'node:assert/strict'
import plugin from '../lib/index.js'

function makeCtx() {
  const handlers = new Map()
  const routes = []
  const ctx = {
    on(name, fn) { handlers.set(name, fn) },
    get(name) { return name === 'webServer' ? { register(r) { routes.push(r); return () => {} } } : undefined },
    // cordis effect: run the factory and keep its disposer
    effect(fn) { return fn() },
    emit(name, ...args) {
      const fn = handlers.get(name)
      if (fn) fn(...args)
    },
  }
  return { ctx, handlers, routes }
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
  const paths = routes.map(r => r.path).sort()
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
