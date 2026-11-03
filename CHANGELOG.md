# Changelog

## 0.1.1 — 2026-11-03

- **修复：热重载后表盘冻结。** 根因是状态被关在 `apply()`/fiber 闭包里，而 Cordis 热重载后路由表可能仍指向旧一代 fiber 注册的 handler（重复路由容错又让旧路由留了下来），客户端因此永远读到重载瞬间冻结的那份快照。状态改为**模块级单例**：任何一代 fiber 的路由都读/写同一份数据，重载后立即恢复实时。
- 客户端传输加固：
  - SSE 与轮询**同时**运行（SSE 求平滑、1.2s 轮询作兜底并以 1.2s 心跳自愈），不再出现"SSE 一断就静默"。
  - 多候选 base（同源 → `__DSH_BOOT__.origin` → http(s) origin），并记录每次尝试的 HTTP 状态与错误。
  - **不再静默失败**：连不上宿主时胶囊直接显示 `未连上宿主 · HTTP 403` 之类；数据超过 8s 未更新显示 `已停`，悬停给出完整传输诊断（mode/base/attempts/http/sse/last payload）。
- 宿主新增 `debug` 计数（apiHits / sseHits / sseActive / lastPushAt / generation / taps），便于判断"客户端到底打没打进来"。
- 单测增至 10 个，新增"跨 fiber 代际状态延续"与"传输失败/数据陈旧可见"用例。

## 0.1.0 — 2026-11-03

- 首个版本：composer dock 常驻 tok/s 胶囊。
- 宿主半挂 `llm/stream`，统计 text / reasoning / tool-call 增量字符并计时。
- 用流结束时的 `usage.outputTokens` 做每模型 chars→tokens EMA 自校准，流中即时速率据此估算。
- 可靠性判据：<500ms 且 <40 token 的短突发调用不输出速率，只给 token 数与耗时。
- `GET /speed-meter/api` 快照 + `GET /speed-meter/api/stream` SSE 推送（400ms 节流，空闲保活）。
- `ctx.effect` 挂路由，fiber 卸除时释放；重复路由容错。
- 调试面：`ctx.emit('speed-meter/snapshot', reply => ...)` 同步取快照；客户端 `exports.__store` 供测试注入快照。
- 7 个单测覆盖路由、增量计数、校准、可靠性、槽位注册与渲染。
