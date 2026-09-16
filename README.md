# monitor

自托管服务器监控：一个 hub（axum + SQLite）加一个轻量 agent，秒级实时数据、公开状态页与后台。

本仓库是 [monitor-probe/monitor](https://github.com/monitor-probe/monitor) 的 fork，分叉点是
`4c53204`（写这份说明时上游 `main` 仍是这个提交），在它之上继续开发。

## 特性

- 实时监控：秒级实时数据展示
- 轻量高效：Rust 语言构建，低资源占用，极简高效
- 自托管：完全掌控数据隐私，部署简单
- 通知：节点上下线、后台登录时推送，渠道可选 PushPlus、SMTP、Telegram 与 Webhook
- 兼容 komari-agent：已装的 komari-agent 在 Hub 添加节点输入原有 token 即可接入，无需改动

## 与上游的差异

### 新增

**komari-agent 兼容接入** —— 在 `/api/clients/v2/rpc` 上实现
[komari](https://github.com/komari-monitor/komari) 的 v2 JSON-RPC（WebSocket 与 gzip POST
两种传输），转换逻辑在 `src/komari_compat.rs`。已部署的 komari-agent 只换一个 token 就能把数据
交到这里。凭据是 `node.komari_token`（可空且唯一，schema v5 新增），与原生 `node.token` 相互
独立：两条链路各自只看自己的凭据列，komari 侧拿不到原生 token，反之亦然。

延迟检测也按会话协议分发。原生 session 连接时收一次 `ping.tasks`，之后自己按 interval 调度；
komari session 收的是 komari 的 `agent.ping`（一次一条，`ping_type` 固定 `tcp`），
因为 komari-agent 每收到一次只测一次，节拍改由 hub 侧按探针的 interval 驱动；没有 socket 的
POST 降级则把探针搭在 `agent.pull` 回复的 `result.events[]` 里。上报侧
`agent.pingResult` 的 `value` 与原生 `ping.result` 的 `latency_ms` 同义，都是负数表示丢包，
两边最终落到同一张 `ping_record`。

**账户密码登录与登录审计**（schema v6）—— 新增 `account` / `login_log` 两张表。除首次运行
打印的应急密码外，可用用户名 + 密码登录（argon2，与应急密码同一条校验路径）；每次登录写一条
审计（时间、方式、账号、IP、设备），GitHub 登录一并记录。面板的安全页可增删账号、翻看登录记录。

**四渠道通知** —— PushPlus 与 SMTP 为新增渠道，与上游已有的 Telegram、Webhook 并列，各带一个
开关，全部挂在总开关 `notify_enabled` 之下；只在该渠道最小配置齐备时才调用，单个渠道失败只记
一条 warn，不影响其他渠道和主进程。各渠道凭据是写-only：设置接口只回 `*_set` 布尔，不回明文。

### 改动

**通知实现整体替换。** 上游那套「队列 + 重试 + 模板 + 防风暴 + 每日摘要」的通知器被移除
（`notes` 通道、`deliver` / `watch` / `expiry_digest` / `renewed` 与 `/api/notify/test`）。
代价说清楚：不再有流量与到期通知，也没有每日摘要，按节点开关通知的语义一并取消。现在只有节点
上下线（`spawn_node_watcher` 按 120 秒心跳窗口判定）和后台登录两类事件，发给四个渠道。
`node.notify` / `node.down_since` 两列保留在 schema 里，`migrate_to_4` 一字未动，好让已经
stamped 4 的库不必在新含义下重跑；Rust 侧不再读写它们。

**`install-hub.sh` 从本仓库的 Release 取产物。** 上游里写死的是 `monitor-probe/monitor`，
在分支上执行装下来的是上游的二进制，与这个仓库发布了什么无关；已改为 `liz963/monitor`。
仓库内其余指向 `monitor-probe` 的地方保持不动，它们指向的确实是上游项目。

### 不变

- **agent 与默认主题仍是上游的**：节点照旧用 [monitor-probe/agent](https://github.com/monitor-probe/agent)，
  主题按 `web-theme.pin` 里的 sha256 从 [monitor-probe/monitor-theme-default](https://github.com/monitor-probe/monitor-theme-default)
  的 Release 校验取用。本仓库只做 hub。
- **schema 只增不减**：迁移链是从上游的 v4 往上接的（v5 加 komari 凭据列、v6 加账号与审计表），
  上游的库和备份可以直接拿过来用。

## 组成

| 仓库 | 说明 |
|---|---|
| [monitor](https://github.com/liz963/monitor) | hub：后台、API、公开页宿主（本仓库） |
| [agent](https://github.com/monitor-probe/agent) | Linux agent（上游） |
| [monitor-theme-default](https://github.com/monitor-probe/monitor-theme-default) | 内置默认主题（上游） |

```
agent (Linux)  ──WebSocket / JSON-RPC 2.0──▶  hub (axum + SQLite)  ──▶  后台 + 状态页
komari-agent   ──WebSocket / gzip POST ────▶  /api/clients/v2/rpc ──▶  同一条上报链路
```
