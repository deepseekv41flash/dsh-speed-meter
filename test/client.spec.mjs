// Client-half smoke test: load lib/client.js in a fake module-loader shell and
// assert the slot registration, then render the pill from pushed snapshots.
import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import vm from 'node:vm'
import { fileURLToPath } from 'node:url'
import path from 'node:path'

const here = path.dirname(fileURLToPath(import.meta.url))
const source = fs.readFileSync(path.join(here, '..', 'lib', 'client.js'), 'utf8')

const plain = () => ({ createElement(type, props, ...children) { return { type, props: props || {}, children: children.flat().filter(c => c != null) } },
  useState(init) { return [init, () => {}] },
  useEffect() {} })

function load() {
  let def = null
  const rendered = []
  const timers = new Map()
  let timerId = 0
  const fetches = []
  const slots = {
    inject(key, effect) { effect() },
    register(reg, render) { rendered.push({ reg, render }); return () => {} },
  }
  const sandbox = {
    window: { __ModuleLoader__: { load: (d) => { def = d } } },
    navigator: { language: 'zh-CN' },
    console,
    // fake timers: the module starts a poller at load time, which must not keep
    // the test process (or the next test) alive
    setTimeout: (fn) => { const id = ++timerId; timers.set(id, fn); return id },
    clearTimeout: (id) => timers.delete(id),
    setInterval: (fn) => { const id = ++timerId; timers.set(id, fn); return id },
    clearInterval: (id) => timers.delete(id),
    EventSource: class { constructor() {} close() {} },
    fetch: async (url) => { fetches.push(String(url)); return { ok: true, status: 200, json: async () => ({ running: null, last: null, recent: [], calibrated: [] }) } },
    JSON, Date, Math, isFinite, String, Number, Object, Array,
  }
  vm.createContext(sandbox)
  vm.runInContext(source, sandbox, { filename: 'client.js' })
  assert.ok(def, 'module loader received a definition')
  assert.equal(def.id, 'dsh-speed-meter')
  const mod = def.factory((name) => {
    if (name === 'react') return plain()
    throw new Error('unexpected require: ' + name)
  })
  return { mod, slots, rendered, fetches }
}

function textOf(node, acc = []) {
  if (node == null) return acc
  if (typeof node === 'string' || typeof node === 'number') { acc.push(String(node)); return acc }
  if (Array.isArray(node)) { for (const n of node) textOf(n, acc); return acc }
  // the fake createElement records components instead of mounting them
  if (typeof node.type === 'function') return textOf(node.type(node.props), acc)
  if (node.children) for (const n of node.children) textOf(n, acc)
  return acc
}

test('exports inject/apply and registers the composer dock entry', () => {
  const { mod, slots, rendered } = load()
  assert.equal(Array.from(mod.inject).join(','), 'slots')
  assert.equal(typeof mod.apply, 'function')
  mod.apply({ get: (k) => (k === 'slots' ? slots : undefined) })
  assert.equal(rendered.length, 1)
  assert.equal(rendered[0].reg.name, 'conversation.composer.dock')
  assert.equal(rendered[0].reg.id, 'speed-meter')
  assert.equal(rendered[0].reg.order, 10)
})

test('scopes requests to the current session id', async () => {
  const { mod, slots, fetches } = load()
  mod.apply({ get: (k) => (k === 'slots' ? slots : undefined) })
  mod.__session.set('sess-A')
  await Promise.resolve()
  await Promise.resolve()
  assert.ok(fetches.some(u => u.indexOf('/speed-meter/api?session=sess-A') >= 0), 'poll carries the session id: ' + fetches.join(','))
  mod.__session.set('sess-B')
  await Promise.resolve()
  await Promise.resolve()
  assert.ok(fetches.some(u => u.indexOf('/speed-meter/api?session=sess-B') >= 0), 'switch re-binds the poll')
  assert.equal(mod.__session.get(), 'sess-B')
})

test('renders nothing on a cold snapshot, a live rate while running, and the exact rate after', () => {
  const { mod, slots, rendered } = load()
  mod.apply({ get: (k) => (k === 'slots' ? slots : undefined) })
  const { render } = rendered[0]

  assert.equal(textOf(render()).join(' ').trim(), '', 'no data => no pill text')

  mod.__store.set({
    running: { sessionId: 's1', model: 'deepseek-flash', tokSec: 100.4, windowTokSec: 120.8, chars: 500, ratio: 3, estTokens: 1500, genMs: 2000 },
    last: null, recent: [], calibrated: [],
  })
  const live = textOf(render()).join(' ')
  assert.match(live, /120\.8 tok\/s/, 'instant window rate is the headline while streaming')

  mod.__store.set({
    running: null,
    last: { sessionId: 's1', model: 'deepseek-flash', tokens: 812, chars: 3244, exact: true, reliable: true, genMs: 5174, tokSec: 156.9 },
    recent: [{ sessionId: 's1', model: 'deepseek-flash', tokens: 812, chars: 3244, exact: true, reliable: true, genMs: 5174, tokSec: 156.9, finishedAt: Date.now() }],
    calibrated: [{ model: 'deepseek-flash', ratio: 0.3, n: 11 }],
  })
  const done = textOf(render()).join(' ')
  assert.match(done, /156\.9 tok\/s/)
  assert.match(done, /812 tok/)
})

test('suppresses the rate for a short burst but keeps the token count', () => {
  const { mod, slots, rendered } = load()
  mod.apply({ get: (k) => (k === 'slots' ? slots : undefined) })
  const { render } = rendered[0]

  mod.__store.set({
    running: null,
    last: { sessionId: 's2', model: 'm', tokens: 43, chars: 28, exact: true, reliable: false, genMs: 36, tokSec: null },
    recent: [], calibrated: [],
  })
  const line = textOf(render()).join(' ')
  assert.match(line, /43 tok/)
  assert.doesNotMatch(line, /tok\/s/)
})

test('surfaces a transport failure instead of rendering nothing', () => {
  const { mod, slots, rendered } = load()
  mod.apply({ get: (k) => (k === 'slots' ? slots : undefined) })
  const { render } = rendered[0]

  mod.__link.attempts = 3
  mod.__link.ok = false
  mod.__link.httpStatus = 403
  const line = textOf(render()).join(' ')
  assert.match(line, /未连上宿主/)
  assert.match(line, /HTTP 403/)
})

test('marks a stale reading so a frozen gauge never looks live', () => {
  const { mod, slots, rendered } = load()
  mod.apply({ get: (k) => (k === 'slots' ? slots : undefined) })
  const { render } = rendered[0]

  mod.__store.set({
    running: null,
    last: { sessionId: 's3', model: 'm', tokens: 1661, chars: 5000, exact: true, reliable: true, genMs: 9000, tokSec: 184.5 },
    recent: [], calibrated: [],
  })
  mod.__link.lastPayloadAt = Date.now() - 12000
  const line = textOf(render()).join(' ')
  assert.match(line, /已停/)
})

test('after a turn ends the pill reports the turn average and usage, not the last call', () => {
  const { mod, slots, rendered } = load()
  mod.apply({ get: (k) => (k === 'slots' ? slots : undefined) })
  const { render } = rendered[0]

  mod.__store.set({
    running: null,
    last: { sessionId: 's', model: 'm', tokens: 80, chars: 300, exact: true, reliable: true, genMs: 400, tokSec: 200 },
    turn: { active: false, startedAt: Date.now() - 9000, endedAt: Date.now(), calls: 3, tokens: 420, chars: 1500, genMs: 2400, wallMs: 9000, exact: true, models: ['m'], tokSec: 175, wallTokSec: 46.7 },
    recent: [], calibrated: [],
  })
  const line = textOf(render()).join(' ')
  assert.match(line, /本轮/)
  assert.match(line, /175\.0 tok\/s/)
  assert.match(line, /420 tok/)
  assert.match(line, /3次/, 'call count shown (' + line + ')')
  assert.doesNotMatch(line, /200\.0 tok\/s/, 'the last single call rate is not the headline')
})

test('while the turn is still running the pill keeps the live instant rate', () => {
  const { mod, slots, rendered } = load()
  mod.apply({ get: (k) => (k === 'slots' ? slots : undefined) })
  const { render } = rendered[0]

  mod.__store.set({
    running: { sessionId: 's', model: 'm', tokSec: 100, windowTokSec: 123.4, chars: 500, ratio: 3, estTokens: 1500, genMs: 2000 },
    last: null,
    turn: { active: true, startedAt: Date.now(), endedAt: null, calls: 1, tokens: 1500, chars: 500, genMs: 2000, wallMs: 2000, exact: false, models: ['m'], tokSec: 750, wallTokSec: 750 },
    recent: [], calibrated: [],
  })
  const line = textOf(render()).join(' ')
  assert.match(line, /123\.4 tok\/s/)
  assert.doesNotMatch(line, /本轮/)
})
