# dsh-speed-meter

[![npm](https://img.shields.io/npm/v/dsh-speed-meter.svg)](https://www.npmjs.com/package/dsh-speed-meter)
[![license](https://img.shields.io/npm/l/dsh-speed-meter.svg)](./LICENSE)

[English](README-en.md) · [简体中文](README.md)

DSH（DeepSeek Harness）实时输出速度计：在输入框下方的 composer dock 里常驻一枚胶囊，流式生成时显示 **实时 tok/s**，调用结束时切成 **该次调用的精确 tok/s**（用 API 上报的 `outputTokens` 除以生成耗时），点开可看最近 12 次调用与每模型的 chars→tokens 自校准系数。

> 生态调研结论（2026-11-03）：DSH 插件市场（7918 个 bundle 索引）与已装插件里都没有 token/s 实时速度类插件，`@feiyang666/dsh-usage-plugin` 只做调用后的 token/费用台账。本插件补这个空位。

## 它显示什么

> 作用域：只显示**当前会话**。胶囊挂在会话内，请求带 `?session=<sessionId>`，宿主按会话维护 last/recent，并对 running 做归属过滤——别的窗口在生成时本会话不会显示别人的速率（tooltip 会提示「其他会话正在生成（不显示）」）。


| 状态 | 胶囊内容 | 含义 |
| --- | --- | --- |
| 流式生成中 | `⚡ 339.5 tok/s` | 最近 1.2 秒滑动窗口的瞬时速率（带自校准系数换算） |
| 等待首字 | `⚡ 等待首字` | 请求已发出、还没收到第一个增量 |
| **一轮结束** | `⚡ 本轮 175.0 tok/s · 420 tok · 3次` | **该轮会话**的平均速率与累计用量：总 `outputTokens ÷ 本轮净生成耗时`，一轮内多步/工具回合全部累加 |
| 无本轮数据时 | `⚡ 156.9 tok/s · 812 tok` | 退回显示上一次调用的精确速率 |
| 短突发调用 | `⚡ 54 tok · 0.6s` | 不输出速率（见下"可靠性判据"） |

轮的边界 = 智能体 `running → idle`；流式过程中胶囊仍显示瞬时速率，tooltip 里另有"本轮累计"。

悬停有完整读数（sessionId / model / purpose / 累计字符数 / ratio / 耗时）；点击展开最近 12 次调用与每模型校准状态。

## 远端模式（DS Harness Remote）

当这台 DSH 作为 **Remote 客户端**连着另一台机器时，模型调用发生在远端，本地 `/speed-meter/api` 没有数据。胶囊会按两条路径取数：

| 路径 | 触发条件 | 数据 |
| --- | --- | --- |
| ① 客户端派生（默认） | 本地宿主半对本会话无数据，但 dock 收到了会话快照 | 已完成步：精确 `outputTokens ÷ (completedTime − firstTokenTime)`；进行中步：块字符增长 × 0.34 估算瞬时速率；`sessionStats` 投影给会话累计 |
| ② 远端 API 代理（可选） | 本地配置了 `remote.webPort` 且 Remote 已连接 | 远端 Host 的 `/speed-meter/api`（含一轮平均、每调用精确值、实时窗口速率、自校准比），与远端 webui 上的数字一致 |

远端代理值在胶囊上带 `⇅` 标记，悬停标题首行写明「数据来源：远端 Host（preview 代理）」。

### 启用 ② 需要三处配合

1. 远端 Host 装好 `dsh-speed-meter`（已具备 `/speed-meter/api`）。
2. 远端 Host 的 `ds-harness-remote` 放行自己的 web 端口，例如 `cordis.patch.yml`：

```yaml
- id: ds-harness-remote
  name: ds-harness-remote
  config:
    loopback:
      ports: [57110]
```

3. 远端 Host 用**固定端口**启动（不能用 `--port 0`），本地 profile 的 patch 指到同一个端口：

```yaml
- id: speed-meter
  name: dsh-speed-meter
  config:
    remote:
      enabled: true
      webPort: 57110
```

失败会记录在 `debug.remote.lastError` 并回退到 ①/本地数据，5 秒后再试。

## 原理

1. **宿主半**挂在 `llm/stream` waterfall 上，包住每一次流式模型调用，逐块统计 `text-delta` / `reasoning-delta` / `tool-call-delta` 的字符数，并记录首个/最后一个增量的时间。
2. 流结束时 API 会给出 `usage` 块（`outputTokens`）。宿主用 `outputTokens ÷ 已统计字符数` 得到该模型**每次调用的精确 chars→tokens 比率**，做 EMA（α=0.2）校准；运行中的即时速率就乘这个校准系数，所以估值会越来越贴近真实 token 数。
3. 客户端半通过 SSE（`/speed-meter/api/stream`，宿主每 400ms 推送一次变化后的完整快照）取数渲染；SSE 不可用时退回轮询（900ms）。

### 可靠性判据

短于 500ms 且 token 少于 40 的调用（会话标题、缓存回放、单块 tool-call 参数）时间分母是传输突发而不是生成耗时，数学上"精确"但会算出上千 tok/s。这类调用 `reliable=false`、`tokSec=null`，UI 只显示 token 数与耗时，不显示速率。

## 安装

本插件由 `dsh.bundle` 装配（host + web client 两半在同一个包里）：

```bash
dsh plugin --profile <profile> add dsh-speed-meter
```

本地开发可把它 link 进 profile 后重启 DSH NEXT；client 半的改动需要刷新页面或重启应用才会重新加载。

## 数据接口

- `GET /speed-meter/api?session=<id>` → 该会话的快照 JSON（不带参数则是进程级最新一次调用），含 `turn`（本轮汇总）：

```json
{
  "running": { "model": "deepseek-flash", "chars": 2063, "estTokens": 613,
               "tokSec": 237.6, "windowTokSec": 339.5, "genMs": 2580, "ratio": 0.3 },
  "last":    { "tokens": 812, "exact": true, "reliable": true, "tokSec": 156.9, "genMs": 5174 },
  "recent":  [ ... ],
  "calibrated": [ { "model": "deepseek-flash", "ratio": 0.3, "n": 11 } ]
}
```

- `GET /speed-meter/api/stream` → SSE，每次变化推送 `data: <同一份 JSON>`；空闲时发注释行保活。
- 宿主内调试：`ctx.emit('speed-meter/snapshot', reply => ...)` 可同步取快照（桌面宿主对普通浏览器关闭了 HTTP 访问时用这条）。
- 快照里的 `debug` 字段给出传输现场：`{ apiHits, sseHits, sseActive, lastHitAt, lastPushAt, generation, taps }`。判断"客户端到底有没有打进来"看 `apiHits/sseHits`；判断"上一代是否还占着路由"看 `generation`。

## 边界与已知限制

- **状态是进程级全局单例**（`globalThis[Symbol.for('dsh-speed-meter/state-v1')]`）。原因：Cordis 热重载会清 ESM 缓存并把新 fiber 挂到旧 fiber 之前，路由表可能仍由上一代 handler 持有；若状态放在模块/fiber 闭包里，表盘会冻结在重载那一刻（这正是 0.1.1 修的 bug）。放进进程级单例后，任何一代 handler 读写的都是同一份实时数据。
- 重复路由的处理：先尝试常规注册；若命中 `duplicate exact route`，就把该路径从 `webServer.exact` 摘掉再重新注册，保证**最新一代 fiber 拥有路由**。
- 页面连接：DSH NEXT 的页面跑在 `dsh-app://app`，页面请求经主进程 `forwardWebRequest` 带 cookie + renderer token 转发到宿主 HTTP，所以插件路由从页面可达；但 SDK 里的 EventSource 连接不会跨 fiber 迁移——已建立的连接仍绑在注册它那一代的 response 对象上，最干净的一次生效需要刷新页面（Ctrl+R）。
- 全部状态在内存里，DSH 重启归零（实时表盘不需要持久化；要台账请用 dsh-usage-plugin）。
- `usage` 只在流正常收尾时上报；被中断/超时的调用拿不到精确 token，只能按校准系数估算并标 `exact=false`。
- 多会话并行时表盘显示"最近活跃的那一次调用"（快照里带 `sessionId` 可区分）。

## 开发

```bash
cd dsh-speed-meter
node --test "test/*.spec.mjs"     # 7 个用例：路由注册、增量计数、校准、可靠性判据、槽位注册、渲染
```

无构建步骤：`lib/index.js` 是纯 ESM 宿主半，`lib/client.js` 是 `window.__ModuleLoader__.load` 形态的 web 客户端半。

## License

MIT
