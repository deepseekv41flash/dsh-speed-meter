# dsh-speed-meter

[![npm](https://img.shields.io/npm/v/dsh-speed-meter.svg)](https://www.npmjs.com/package/dsh-speed-meter)
[![license](https://img.shields.io/npm/l/dsh-speed-meter.svg)](./LICENSE)

[English](README-en.md) · [简体中文](README.md)

A real-time output-speed meter for DSH (DeepSeek Harness): a pill sits in the composer dock below the input, showing **live tok/s** while a model streams and switching to the **exact tok/s of that call** (API-reported `outputTokens` ÷ generation time) when it settles. Click it for the last 12 calls and the per-model chars→tokens calibration.

> Ecosystem check (2026-11-03): none of the 7918 bundles in the DSH plugin market, nor any installed plugin, shows real-time output speed (`token/s`). `@feiyang666/dsh-usage-plugin` only keeps a post-hoc token/cost ledger. This plugin fills that gap.

## What it shows

| State | Pill | Meaning |
| --- | --- | --- |
| Streaming | `⚡ 339.5 tok/s` | Instant rate over a 1.2 s sliding window, scaled by the learned ratio |
| Waiting | `⚡ waiting for first token` | Request sent, no delta yet |
| Settled | `⚡ 156.9 tok/s · 812 tok` | **Exact** rate for that call: `outputTokens / generation time` |
| Short burst | `⚡ 54 tok · 0.6s` | No rate reported (see reliability rule) |

Hovering shows the full readout (sessionId / model / purpose / chars / ratio / elapsed); clicking expands the last 12 calls and the calibration state.

## How it works

1. **Host half** taps the `llm/stream` waterfall, wrapping every streaming model call, counting `text-delta` / `reasoning-delta` / `tool-call-delta` characters and timing the first/last delta.
2. When the stream ends the API reports a `usage` block (`outputTokens`). The host divides it by the counted characters to obtain that model's **exact chars→tokens ratio** and keeps it as an EMA (α=0.2). Live estimates multiply by that calibration, so they converge on real tokens.
3. **Web client half** consumes an SSE stream (`/speed-meter/api/stream`, a full snapshot pushed every 400 ms when it changes), falling back to 900 ms polling.

### Reliability rule

Calls shorter than 500 ms with fewer than 40 tokens (session titles, cached replays, single-burst tool-call arguments) have a transport burst as the time denominator — mathematically "exact" but nonsense (thousands of tok/s). Those get `reliable=false`, `tokSec=null`, and the UI shows only tokens and duration.

## Install

Ships as a `dsh.bundle` (host + web client in one package):

```bash
dsh plugin --profile <profile> add dsh-speed-meter
```

For local development, link it into the profile and restart DSH NEXT; client-half changes need a page reload or app restart to be re-loaded.

## API

- `GET /speed-meter/api` — current snapshot JSON (running / last / recent / calibrated).
- `GET /speed-meter/api/stream` — SSE, one `data: <snapshot JSON>` per change, comment keepalive when idle.
- In-host debugging: `ctx.emit('speed-meter/snapshot', reply => ...)` returns the snapshot synchronously — useful when the desktop host blocks ordinary browser HTTP access.
- The snapshot's `debug` field shows the transport现场: `{ apiHits, sseHits, sseActive, lastHitAt, lastPushAt, generation, taps }` — `apiHits/sseHits` tell whether the client reached the host at all; `generation` tells whether a previous fiber still owns a route.

## Limits

- **State is a process-global singleton** (`globalThis[Symbol.for('dsh-speed-meter/state-v1')]`). A Cordis hot reload clears the ESM cache and mounts the replacement fiber before retiring the old one, so the route table can still hold the previous generation's handler; state kept in a module/fiber closure would then be frozen at the reload instant (the 0.1.1 bug). With one process-global state every generation serves the same live data.
- Duplicate routes: register normally; on `duplicate exact route`, drop the path from `webServer.exact` and register again so the newest fiber owns the route.
- Page transport: the DSH NEXT page runs on `dsh-app://app`; its requests are forwarded by the main process (`forwardWebRequest`) with the cookie and renderer token, so plugin routes are reachable from the page. An already-open EventSource stays bound to the response of the generation that created it, so the cleanest activation is one page reload (Ctrl+R).
- Everything is in memory; a DSH restart resets it (a live gauge needs no persistence — for ledgers use dsh-usage-plugin).
- `usage` only arrives when a stream finishes normally; interrupted calls fall back to the calibrated estimate and are marked `exact=false`.
- With several sessions streaming, the gauge shows the most recent call (snapshots carry `sessionId`).
- On host hot-reload, a route leaked by the previous fiber triggers `duplicate exact route`; the plugin tolerates it (keeps the old route) and a full cleanup needs an app restart.

## Development

```bash
cd dsh-speed-meter
node --test "test/*.spec.mjs"     # 7 cases: routes, delta counting, calibration, reliability, slot registration, rendering
```

No build step: `lib/index.js` is plain-ESM host code, `lib/client.js` is a `window.__ModuleLoader__.load` web client module.

## License

MIT
