# Changelog

## 0.2.0 — 2026-11-04

- **新增：每轮会话结束后显示"本轮"的平均输出速度与累计用量**（此前只显示上一次调用的数字）。
  - 宿主监听 `agent/status` 的 `running ⇄ idle` 作为轮边界；一轮内所有模型调用（多步、工具回合、重试）都累加进该轮。
  - 快照新增 `turn` 字段：`{ active, calls, tokens, chars, genMs, wallMs, exact, models, tokSec, wallTokSec }`。`tokSec = 总 tokens ÷ 本轮净生成耗时`（不含工具执行等待），`wallTokSec` 用墙钟时间另给一份。
  - 客户端空闲时优先显示本轮汇总：`⚡ 本轮 175.0 tok/s · 420 tok · 3次`；tooltip 给出本轮 token/调用次数/净生成耗时/墙钟/模型/精确或估算，并保留"上次调用"作为对照。
  - 生成过程中仍显示实时瞬时速率，tooltip 追加"本轮累计"。
  - 没有收到 `running` 状态的调用（插件中途加载、标题/摘要调用）会开一个隐式轮，不会丢数据。
- 会话 id 两种写法（裸 uuid / `session-` 前缀）统一归一化后再归档。
- 单测增至 22 个（本轮聚合、隐式轮、本轮胶囊渲染、轮内实时速率不被覆盖）。

## 0.1.5 — 2026-11-03

- 仓库文档格式对齐其他 DSH 插件：`README.md` 改为**中文为主**，新增 `README-en.md` 英文版，两份第 6 行都带语言切换链接 `[English](README-en.md) · [简体中文](README.md)`；同时补 npm/license badge。
- `package.json` 的 `files` 同步为 `README.md` + `README-en.md`（发布件里的包说明因此为中文）。
- 无代码改动。

## 0.1.4 — 2026-11-03

- 新 tap 上岗即退掉旧 tap：HMR 瞬间新旧两代 `llm/stream` 监听器可能同时存活，同一次调用会被 finalize 两次（`recent` 里出现完全相同的两行）。现在 `apply()` 会先 dispose 上一代表针再装新的。
- `recent` 写入去重：同一会话/模型/`tokens`/`chars`/`genMs` 完全一致的相邻记录合并为一行（防御多 tap 并存的残余场景）。
- 单测增至 18 个（tap 交接只留一个、重复行折叠）。

## 0.1.3 — 2026-11-03

- **修复：插件会让本轮运行失败**（`TypeError: Cannot read properties of undefined (reading 'get')`，出现在 `sessionState` → `finalize`）。根因是 0.1.2 给进程级全局状态单例**新增了 `sessions` 字段**，而热重载前留下的旧形状对象没有该字段，`st.sessions.get(...)` 直接抛错；异常发生在 `llm/stream` 包装流的 `finally` 里，于是把整轮模型调用判失败。修法两层：
  1. `state()` 对全局单例做**逐字段形状迁移/兜底**（`sessions`、`ratioByModel`、`recent`、`debug` 计数等缺失即补齐）——升级版本时不得假设旧对象已有新字段；
  2. 指标插件永不拖垮 turn：`finalize()` 全程 `try/catch`，异常只记入 `debug.lastError`，绝不向模型流抛出。
- 单测增至 16 个，新增「旧版全局状态迁移」与「per-session 存储抛错也不能让 turn 失败」两条回归。

## 0.1.2 — 2026-11-03

- **表盘只显示当前会话**：composer dock 的 `sessionId` prop 现在随每个请求带上（`?session=<id>`），宿主按会话维护 `last`/`recent`，快照对 `running` 也做归属过滤——别的窗口在生成时，本会话不再显示别人的速率。
- 会话切换会清空并重连（SSE 重开、轮询改参数），不会残留上一个会话的读数。
- 其他会话正在生成时，tooltip 会提示「其他会话正在生成（不显示）」。
- 宿主调试通道支持 `ctx.emit('speed-meter/snapshot', sessionId, reply)` 取会话视角快照。
- 单测增至 14 个（新增会话隔离、跨会话 running 不泄漏两条）。

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
