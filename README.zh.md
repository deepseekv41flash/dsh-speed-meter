# dsh-speed-meter

DSH（DeepSeek Harness）实时输出速度计：在输入框下方的 composer dock 里常驻一枚胶囊，流式生成时显示 **实时 tok/s**，调用结束时切成 **该次调用的精确 tok/s**（用 API 上报的 `outputTokens` 除以生成耗时），点开可看最近 12 次调用与每模型的 chars→tokens 自校准系数。

> 生态调研结论（2026-11-03）：DSH 插件市场（7918 个 bundle 索引）与已装插件里都没有 token/s 实时速度类插件，`@feiyang666/dsh-usage-plugin` 只做调用后的 token/费用台账。本插件补这个空位。

## 它显示什么

| 状态 | 胶囊内容 | 含义 |
| --- | --- | --- |
| 流式生成中 | `⚡ 339.5 tok/s` | 最近 1.2 秒滑动窗口的瞬时速率（带自校准系数换算） |
| 等待首字 | `⚡ 等待首字` | 请求已发出、还没收到第一个增量 |
| 调用结束 | `⚡ 156.9 tok/s · 812 tok` | 该次调用的**精确**速率：`outputTokens / 生成耗时` |
| 短突发调用 | `⚡ 54 tok · 0.6s` | 不输出速率（见下"可靠性判据"） |

悬停有完整读数（sessionId / model / purpose / 累计字符数 / ratio / 耗时）；点击展开最近 12 次调用与每模型校准状态。

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

- `GET /speed-meter/api` → 当前快照 JSON：

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

## 边界与已知限制

- 全部状态在内存里，DSH 重启归零（实时表盘不需要持久化；要台账请用 dsh-usage-plugin）。
- `usage` 只在流正常收尾时上报；被中断/超时的调用拿不到精确 token，只能按校准系数估算并标 `exact=false`。
- 多会话并行时表盘显示"最近活跃的那一次调用"（快照里带 `sessionId` 可区分）。
- 宿主半热重载时若上一个 fiber 的路由没释放，注册会命中 `duplicate exact route`；插件对此容错（保留旧路由），完整清理需要重启应用。

## 开发

```bash
cd dsh-speed-meter
node --test "test/*.spec.mjs"     # 7 个用例：路由注册、增量计数、校准、可靠性判据、槽位注册、渲染
```

无构建步骤：`lib/index.js` 是纯 ESM 宿主半，`lib/client.js` 是 `window.__ModuleLoader__.load` 形态的 web 客户端半。

## License

MIT
