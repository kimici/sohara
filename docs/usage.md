# Sohara 使用文档

> 适用版本：S0–S7 + 分布式 D1–D6 全量实现。设计背景见 [`redesign-and-roadmap.md`](design/redesign-and-roadmap.md)（单机路线）与 [`distributed-plane-and-dashboard.md`](design/distributed-plane-and-dashboard.md)（分布式管理层）；YAML 语法见 [`yaml-workflow-schema.md`](design/yaml-workflow-schema.md)，脚本 API 见 [`quickjs-api.md`](design/quickjs-api.md)。

## 0. 构建

```console
$ cargo build --release -p sohara-cli        # 单机二进制（target/release/sohara）
$ cargo build --release -p sohara-agent      # 节点代理
$ cargo build --release -p sohara-plane      # 控制面（Gateway + Manager + 中继）
```

> 沙箱环境需先 `export CARGO_HOME=$PWD/.cargo-home`。
> 本机若配置了 HTTP 代理，本地 HTTP 访问已在各客户端内置 `no_proxy`，无需处理。

## 1. 单机使用

### 1.1 快速上手

```console
$ sohara init demo && cd demo
$ sohara run flow.yaml
Flow 'basic' finished: processed=2, filtered=1, errors=0, waiting=0, duplicates=0
```

### 1.2 命令一览

| 命令 | 说明 |
|---|---|
| `sohara init [dir]` | 生成 `flow.yaml` + `data/input.csv` 骨架 |
| `sohara run <flow.yaml> [--resume] [--verbose] [--history PATH]` | 一次性运行；`--resume` 复用存储的 run_id（幂等续跑）；`--verbose` 打印步骤统计表；每次运行写入 `.sohara/history.jsonl`（失败运行记 `status=error`） |
| `sohara serve <flow.yaml> [--admin ADDR] [--admin-token T] [--resume] [--relay URL] [--relay-token T]` | 常驻运行触发器（http/cron/queue）直至 Ctrl+C/SIGTERM；停机时优雅排水并写一条历史 |
| `sohara approve <flow.yaml> [--step ID]` | 放行 `approve` 步骤停放的记录 |
| `sohara history [--limit N] [--history PATH]` | 查看运行历史 |

### 1.3 单机 Dashboard

`serve` 模式加 `--admin` 后：

- `GET /admin/health`、`/admin/metrics`、`/admin/status`（流程/触发器/步骤统计/paused）、`/admin/approvals`、`/admin/errors`、`/admin/history`
- `POST /admin/pause`、`POST /admin/resume`——协作式暂停：暂停后已拉取的记录被握住不处理，背压向上游传播
- **`GET /admin/ui`**：浏览器打开内嵌 Dashboard（概览卡片、步骤表、触发器、审批队列、错误流、历史；3 秒轮询）
- 设置 `--admin-token` 后所有 `/admin/*` 需要 `Authorization: Bearer <token>`

```console
$ sohara serve examples/serve.yaml --admin 127.0.0.1:9528 --admin-token sekret
$ curl -H "Authorization: Bearer sekret" http://127.0.0.1:9528/admin/status
```

### 1.4 持久化 / 恢复 / 审批

```yaml
checkpoint: { store: state/orders.json, every: 500 }   # 状态存储 + 每 500 条检查点
steps:
  - { id: gate, kind: control, type: approve, config: { title: "大额审批", owners: [alice] } }
```

- `run --resume`：复用存储 run_id，delivered 幂等键去重（内容哈希或 `metadata.idempotency_key`）。
- `approve` 无 store 时降级放行；有 store 时停放，`sohara approve` 放行后从停放点续跑。
- 幂等边界：delivered 键随 run_id 变化，**跨重启去重依赖业务幂等键**。

### 1.5 连接器与脚本（示例见 `examples/`）

- `source/sink: file`（csv/json/jsonl）、`source/sink: db`（SQLite，`{path, query}` / `{path, table}`）、`source/sink: http`（`{url, method?, headers?, poll_interval?}`）、`source/transform/sink: script`（QuickJS，`{script | inline, entry?}`）。
- 复用片段：`imports: [parts/common.yaml]` + 模板 `templates:` + 步骤 `use: <模板名>`（嵌套 config 深合并）。
- 完整示例索引见 [`examples/README.md`](../examples/README.md)。

## 2. 分布式使用（plane + agent）

### 2.1 架构速览

```
外部请求 ──▶ sohara-plane（Gateway /gw + Manager /ui + 中继 /relay + 对账）
                ▲ 心跳/命令（agent 拨出）
          sohara-agent（每机一个）── 进程级管理 ──▶ sohara serve 实例（--admin --relay）
```

### 2.2 启动控制面

```console
$ sohara-plane --addr 127.0.0.1:9600 --state plane-state.json [--token plane-tok]
```

设置 `--token` 后，`/api/*`、`/agent/*`、`/relay/*`、`/ui` 全部需要 Bearer 认证；`/gw/*` 是免 token 的外部统一入口。

### 2.3 配置并启动 agent

```yaml
# agent.yaml
node: { id: n1 }
plane: { url: "http://127.0.0.1:9600", token: plane-tok }   # 与 --token 一致
heartbeat_ms: 1000
instances: []        # 可留空：实例由 plane 声明下发（推荐）
```

```console
$ sohara-agent agent.yaml
```

agent 会：每 1s 本地健康检查实例 `/admin/health`、每 5s 心跳上报；崩溃/健康失败按 policy 指数退避重启（封顶 60s，超预算标记 failed）；plane 不可达时保持现状运行（keep-running）。

### 2.4 声明实例（Manager API）

```console
$ curl -H "Authorization: Bearer plane-tok" -X POST http://127.0.0.1:9600/api/instances \
  -H "Content-Type: application/json" -d '{
    "id": "orders-1",
    "node": "n1",
    "flow_id": "orders",            # 路由分组（gateway 路由按它匹配）
    "desired": "running",           # running | paused | stopped
    "spec": {
      "id": "orders-1",
      "flow": "/srv/flows/orders.yaml",   # 节点上的 flow 文件路径
      "bin": "sohara",
      "admin": "127.0.0.1:9528",          # 实例管理端口（心跳上报用）
      "admin_token": "instance-tok",
      "trigger": "127.0.0.1:9527",        # http 触发器地址（gateway 转发目标）
      "relay": "http://127.0.0.1:9600",   # 事件总线桥接（跨机通信）
      "health_enabled": true,
      "policy": { "restart": true, "max_restarts": 5, "backoff_ms": 2000, "health_failures": 3 }
    }
  }'
```

声明式生命周期：改 `desired` 即收敛——`PUT /api/instances/:id/desired {"desired":"stopped"}` 停机、再改回 `running` 拉起；`DELETE /api/instances/:id` 撤销声明。spec 更新会自动替换进程（重启走 `--resume` 复用 run_id）。

其余端点：`GET /api/nodes`、`GET /api/instances`（desired+actual 合并视图）、`GET /api/instances/:id/status`（直查实例实时状态，透传 admin token）、`GET/POST /api/routes`、`DELETE /api/routes/:id`、`GET /api/events`（集群事件历史）、`GET/POST /api/flows`。

### 2.5 Gateway 路由与调度

```console
$ curl -H "Authorization: Bearer plane-tok" -X POST http://127.0.0.1:9600/api/routes \
  -H "Content-Type: application/json" -d '{
    "id": "r1", "path": "/webhook/orders", "flow_id": "orders",
    "mode": "proxy", "strategy": "round_robin", "sticky_key": "X-Order-Id"
  }'
$ curl -X POST http://127.0.0.1:9600/gw/webhook/orders  # 转发到候选实例触发器
```

- `mode: proxy`（默认）：反向代理到实例 http 触发器，同步请求-响应；候选 = `actual=running` 且有 `trigger` 地址的实例；失败重试下一个候选（共 2 个），全挂 503。
- `mode: bus`：显式声明，发布即返回 202——请求体进入 `topic` 的中继邮箱，由订阅该 topic 的实例 queue 触发器竞争消费（异步任务）。
- `strategy: round_robin | hash`；hash 对 `sticky_key` 头做确定性排序（弱 sticky：实例增减允许漂移，正确性靠业务幂等键）。`tags/least-loaded` 延后。

### 2.6 跨机事件总线（实例间通信）

实例只需在声明里带 `relay` 地址（启动时等价于 `sohara serve --relay <plane>`）：

- 发布：流程里的 `sink: { type: queue, config: { topic: orders.events } }` 自动本地扇出 + 转发 plane；
- 消费：`triggers: [{ id: bus, type: queue, topic: orders.events }]` 原样消费（plane 每 500ms 推送注入本地总线）。

投递语义：**尽力投递**，每主题有界 1000 条（超限丢最旧），每批 100 条按游标增量。plane 按稳定订阅者 id（实例 admin 地址）保存游标下限——**实例重启不重放已确认消息**；plane 重启会丢内存游标（重放邮箱尾部），消费端应以业务幂等键消重；持久化/至少一次待 D5b（NATS/JetStream，可选后期项）。

### 2.7 Manager UI（全局 Dashboard）

浏览器打开 `http://127.0.0.1:9600/ui`（需 Bearer token）：

- **Nodes / Instances**：节点心跳与实例矩阵（desired/actual/healthy/restarts），每行 run/pause/stop/删除按钮；
- **Declare instance**：表单声明新实例（spec 为 JSON 文本框）；
- **Routes**：路由表增删；
- **Event history**：集群事件流（声明、desired 变更、状态迁移）；
- 实例详情按钮 → 经 plane 直查实例 `/admin/status` 实时数据。

### 2.8 安全

三个信任域、三向 token（均为可选项，建议生产全开）：

| 通道 | token | 校验方 |
|---|---|---|
| plane ↔ agent（心跳/命令） | `plane --token` / `agent.yaml plane.token` | plane |
| plane ↔ 实例 admin（状态直查） | 声明中的 `spec.admin_token` | 实例 |
| 实例 ↔ plane relay | `spec.relay_token` / `serve --relay-token` | plane（与 plane token 同中间件） |

单机 admin UI 默认仅显式 `--admin` 开启，建议绑定 loopback/内网。mTLS、控制面 HA、Gateway 前置 LB 为延后增强（单点已接受，见设计文档 §9）。

## 3. CI 与发布

- **每次 push / PR**：`.github/workflows/ci.yml` 运行全量验证——`cargo fmt --check`、`cargo clippy --all-targets -D warnings`、`cargo build --workspace --locked`、`cargo test --workspace --locked`、文件/函数长度门禁。
- **打 tag 发布**：推送 `vXX.YY.ZZ` 或 `vXX.YY.ZZ-AAA` 格式的 tag（如 `v0.2.0`、`v0.2.0-alpha`）触发 `.github/workflows/release.yml`——按 matrix 构建四个平台并发布到 GitHub Release（自动生成 release notes）：

| 平台 | 产物 |
|---|---|
| windows x64 | `sohara-win-x64.zip` |
| linux x64 | `sohara-linux-x64.tar.gz` |
| linux arm64 | `sohara-linux-arm64.tar.gz` |
| macOS arm64 | `sohara-apple-darwin-arm64.tar.gz` |

每个产物包含 `sohara`、`sohara-agent`、`sohara-plane` 三个二进制。

```console
$ git tag v0.2.0-alpha && git push origin v0.2.0-alpha
```

> 注：`vXX.YY.ZZ` 无后缀同样触发发布；其他格式（如 `v1.2.3-alpha.1` 带点的后缀）不会触发发布，但仍会运行 ci.yml 的全量验证。

## 4. 常见问题排查

- **本地 HTTP 502 / 连接失败**：检查 `HTTP_PROXY` 环境变量——agent/plane/relay 客户端已内置 `no_proxy`，但外部 `curl` 请加 `--noproxy '*'`。
- **实例反复 restarting**：多为端口冲突（`--admin` 与触发器端口勿相同）或 `bin` 路径错误；看 agent 日志的 `[id] spawn failed` 与 `restart budget spent`。
- **文件 sink 看不到输出**：file sink 缓冲记录、仅在优雅停机（flush）时落盘——验证时先 SIGTERM 再看文件；需要实时可见请用 `log` sink。
- **跨机消息未到达**：确认消费实例声明了 `relay` 地址、queue 触发器 topic 与发布 topic 一致；plane 日志可见 `/relay/publish` 202；用 `/api/events` 与实例 `/admin/errors` 排查。
- **gateway 404**：路径需以声明路由的 `path` 为前缀（`/gw` 后拼接）；`mode: bus` 的路由缺 `topic` 返回 400。
- **健康检查失败计数**：`policy.health_failures` 默认 3，本地探测 1s 一次，约 3s 判定 unhealthy 并按 `backoff_ms` 退避重启。
