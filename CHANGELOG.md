# Changelog

## 0.1.0 — 2026-11-03

- 首个版本：composer dock 常驻 tok/s 胶囊。
- 宿主半挂 `llm/stream`，统计 text / reasoning / tool-call 增量字符并计时。
- 用流结束时的 `usage.outputTokens` 做每模型 chars→tokens EMA 自校准，流中即时速率据此估算。
- 可靠性判据：<500ms 且 <40 token 的短突发调用不输出速率，只给 token 数与耗时。
- `GET /speed-meter/api` 快照 + `GET /speed-meter/api/stream` SSE 推送（400ms 节流，空闲保活）。
- `ctx.effect` 挂路由，fiber 卸除时释放；重复路由容错。
- 调试面：`ctx.emit('speed-meter/snapshot', reply => ...)` 同步取快照；客户端 `exports.__store` 供测试注入快照。
- 7 个单测覆盖路由、增量计数、校准、可靠性、槽位注册与渲染。
