/**
 * dsh-speed-meter — remote bridge (host half helper).
 *
 * When the local Harness is connected to another machine through
 * `ds-harness-remote` in client mode, the running model calls happen on that
 * remote Host; the local `/speed-meter/api` has nothing to report. The Remote
 * plugin exposes a client runtime on `ctx.get('dshRemoteClient')` whose
 * `preview.open(port)` maps one remote loopback HTTP port onto a private local
 * origin (HTTP + WS proxied through the authenticated channel). Pointing that
 * proxy at the REMOTE Host's web server reaches the remote speed-meter routes,
 * so the local pill can show the remote numbers with no protocol changes.
 *
 * Requirements on the remote Host (documented in README):
 *  - `dsh-speed-meter` installed in its profile (it serves /speed-meter/api),
 *  - `ds-harness-remote` configured with `loopback.ports: [<its web port>]`,
 *  - the Host started on a FIXED web port (not `--port 0`).
 */

/** A preview URL is private to this machine; keep it for the session lifetime. */
export class RemoteBridge {
  /**
   * @param ctx - the plugin context (used only for `ctx.get('dshRemoteClient')`).
   * @param options - `{ enabled, webPort, pollMs }` from the plugin config.
   */
  constructor(ctx, options) {
    this.ctx = ctx
    const cfg = options || {}
    this.enabled = cfg.enabled !== false && Number.isInteger(cfg.webPort) && cfg.webPort > 0
    this.webPort = Number.isInteger(cfg.webPort) ? cfg.webPort : 0
    this.pollMs = Number.isInteger(cfg.pollMs) && cfg.pollMs >= 250 ? cfg.pollMs : 700
    this.url = null
    /** Negative cache: a broken preview must not slow every snapshot request. */
    this.retryAfter = 0
    this.debug = {
      enabled: this.enabled,
      connected: false,
      mode: 'local',
      port: this.webPort || null,
      url: null,
      ok: false,
      lastError: '',
      lastAt: 0,
    }
  }

  client() {
    try {
      const client = this.ctx.get('dshRemoteClient')
      return client && typeof client === 'object' ? client : undefined
    } catch (e) {
      return undefined
    }
  }

  /** Client-side connection state, refreshed on every call. */
  state() {
    const client = this.client()
    if (!client || typeof client.status !== 'function') {
      this.debug.connected = false
      this.debug.mode = 'unavailable'
      return this.debug
    }
    let status
    try {
      status = client.status()
    } catch (e) {
      this.debug.mode = 'unknown'
      this.debug.lastError = String((e && e.message) || e)
      return this.debug
    }
    this.debug.mode = status && status.mode ? String(status.mode) : 'unknown'
    this.debug.connected = !!(status && status.mode === 'remote' && status.connected)
    return this.debug
  }

  /** Resolve (and memoize) the local preview origin for the remote web port. */
  async baseUrl() {
    if (this.url) return this.url
    const client = this.client()
    if (!client) throw new Error('remote client runtime unavailable')
    const state = this.state()
    if (!state.connected) throw new Error('rewire: remote is not connected (mode=' + state.mode + ')')
    const preview = client.preview
    if (!preview || typeof preview.open !== 'function') throw new Error('remote preview API unavailable')
    const opened = await preview.open(this.webPort)
    const url = opened && typeof opened.url === 'string' ? opened.url : ''
    if (!url) throw new Error('preview.open returned no url')
    this.url = url.replace(/\/$/, '')
    this.debug.url = this.url
    return this.url
  }

  /** Drop the memoized origin (transport loss, Host restart). */
  reset() {
    this.url = null
    this.debug.url = null
  }

  /**
   * Fetch one snapshot from the remote speed-meter.
   *
   * @param sessionId - the remote session the dock is mounted in (may be empty).
   * @returns the remote snapshot JSON (already the same shape as the local one).
   */
  async fetchSnapshot(sessionId) {
    if (Date.now() < this.retryAfter) throw new Error('remote bridge cooling down: ' + this.debug.lastError)
    try {
      const base = await this.baseUrl()
      return await this.read(base, sessionId)
    } catch (e) {
      this.retryAfter = Date.now() + 5000
      this.reset()
      throw e
    }
  }

  /** One authenticated read through the preview origin. */
  async read(base, sessionId) {
    const query = sessionId ? '?session=' + encodeURIComponent(sessionId) : ''
    const response = await fetch(base + '/speed-meter/api' + query, { cache: 'no-store' })
    this.debug.lastAt = Date.now()
    if (!response.ok) throw new Error('remote speed-meter HTTP ' + response.status)
    const json = await response.json()
    this.debug.ok = true
    this.debug.lastError = ''
    return json
  }
}
