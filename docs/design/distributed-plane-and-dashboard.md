# Sohara 分布式管理层与 Dashboard 设计

> 状态：**v2**（已按 challenge 修订）。范围对齐既有定稿：单机优先、分布式为**高层实现**、AI agent 不实现（见 `redesign-and-roadmap.md` §3 与 `extension-points.md`）。

## 0. 决策记录（challenge 后定稿）

| # | 决策点 | 结论 | 理由 |
|---|---|---|---|
| D1 | 实例间通信总线 | **内置中转（PlaneRelayBus）起步，NATS 作为后期选项** | 零外部依赖、保持单二进制部署体验；NATS 规模价值在小集群不兑现。`EventBus` trait 不变，后期可无痛替换 |
| D2 | agent 管理实例方式 | **进程级（spawn `sohara serve --admin`）** | 隔离与崩溃安全最好，复用 release 二进制与全部单机能力；重启恢复语义显式化（见 §5） |
| D3 | Gateway 默认分发模式 | **proxy 为主，bus 显式声明** | 默认同步请求-响应；`mode: bus` 的路径才走总线（异步语义，发布即返回） |

## 0.1 目标与非目标

**目标**（对应需求 1–5）：

1. 上层控制多台机器上的 sohara 单机实例（任务下发/启停/重启/暂停），并支持实例间通信。
2. 上层把外部请求调度到不同实例。
3. 上层管理 sohara 生命周期，基于运行状态（监控）决定启动/停止/重启。
4. 上层暴露统一出口（Gateway）与管理界面（Manager）。
5. sohara 以 server 模式运行时自带 Dashboard，可查看本实例工作状态与任务详情；全局 Manager 提供跨实例 Dashboard。

**非目标（明确排除）**：控制面多副本 HA/选举、自动扩缩容、分布式事务、agent 自治编排、AI agent。

## 1. 总体架构

```
                        ┌────────────────────────────────────────────┐
  外部请求 ──────────▶  │  sohara-plane（控制面，单实例进程）          │
                        │  ┌──────────┐  ┌──────────┐  ┌───────────┐ │
                        │  │ Gateway  │  │ Manager  │  │ Scheduler │ │
                        │  │ 统一入口  │  │ API+全局UI│  │ 路由/生命周期│ │
                        │  └────┬─────┘  └────┬─────┘  └─────┬─────┘ │
                        │       └──────┬─────┴───────────────┘       │
                        │         Registry（期望状态唯一来源）          │
                        └──────┬───────┴───────────┬─────────────────┘
                    控制通道（agent 拨出：心跳+指标上报、命令拉取；HTTP/JSON+token）
                ┌──────────────┴──────┐    ┌───────┴──────────────┐
                │  sohara-agent (机器A)│    │  sohara-agent (机器B) │
                │  实例进程管理/健康检查│    │  同左                  │
                │   ┌──────────────┐  │    │   ┌──────────────┐   │
                │   │sohara serve  │  │    │   │sohara serve  │   │
                │   │ --admin 9528 │  │    │   │ --admin 9528 │   │
                │   └──────┬───────┘  │    │   └──────┬───────┘   │
                └──────────┼──────────┘    └──────────┼───────────┘
                           │       数据通道（D5a：plane 中转；D5b：NATS）│
                           └─────────────────────────────────────────┘
```

组件职责：

| 组件 | 职责 |
|---|---|
| `sohara-agent`（每机一个守护进程） | 管理本机 sohara 实例进程；本地健康检查（1s 节拍）；心跳/指标上报（5s）；执行 plane 命令；转发总线消息 |
| `sohara-plane` | Registry（期望状态唯一来源）+ Manager API（部署/生命周期/监控查询）+ Scheduler（路由、健康策略）+ Gateway（统一入口）+ 总线中转（D5a） |
| 数据通道 | 实例间通信（§2.2）：D5a 内置中转，D5b NATS（后期） |
| 单机 Dashboard | serve 模式内嵌 `/admin/ui`（§6） |
| 全局 Dashboard | Manager 提供的 Web UI（§7） |

## 2. 通信设计

### 2.1 控制通道（agent ↔ plane）

- **方向**：agent 主动拨出（心跳 + 命令队列拉取），plane 不主动连 agent。
- **协议**：HTTP/JSON 起步（复用 axum/reqwest）；抽象 `ControlTransport` trait，将来可替换 gRPC/QUIC。
- **认证（提前到 D2）**：agent↔plane 双向 token；单机 admin API 增加 `--admin-token`（D1），agent 用同一 token 访问实例。token 缺失时管理端点 401。
- **两条节拍分离**：
  - 本地健康检查：agent 每 **1s** 探测实例 `/admin/health`（轻量），快故障秒级发现（plane 决策允许滞后，见 §5）。
  - 心跳上报：每 5s 向 plane 上报实例状态 + 指标快照（`RunReport` 字段、资源粗值、队列深度——D1 后才有）。
- **命令模型**：plane 写 agent 命令队列（`{op, instance, seq}`）；agent 拉取执行并回执；`seq` 重连去重。对账兜底（§3）保证命令丢失后状态仍收敛。

### 2.2 数据通道（实例 ↔ 实例）

- **D5a（本版实现）：PlaneRelayBus（内置中转）**
  - `sink.queue` 发布 → 本机 agent 上报 plane → plane 按订阅表推给订阅实例所在 agent → agent 注入该机 `InProcessBus` → 既有 `queue` 触发器原样消费。
  - 语义：**异步、尽力投递**——plane 为每个订阅实例持有界队列，积压超限丢弃并告警（背压语义与单机有界通道一致）；**不提供 request-reply**。
  - 代价（明确接受）：plane 承担数据面，是单点与瓶颈；持久化/至少一次留待 NATS。
- **D5b（后期可选）：NATS/JetStream**
  - 新增 `NatsBus` 实现同一 `EventBus` trait + `nats` 触发器；JetStream 提供持久化、至少一次、积压保留；配合单机幂等键消重；需要时提供 request-reply。
  - 路由语义不变（订阅表 → subject 映射），单机代码零改动。

## 3. 核心模型（Registry）

```yaml
nodes:
  - { id: n1, addr: 10.0.0.11:9529, tags: [zone-a, gpu] }

flows:
  - id: flow-orders
    name: orders
    yaml: ./flows/orders.yaml     # plane 持有内容并分发

instances:
  - id: orders-1
    node: n1
    flow: flow-orders
    desired: running              # running | paused | stopped
    policy: { restart: always, max_restarts: 5, backoff: 2s, health_failures: 3 }
    routing: { weight: 2, sticky_key: order_id }
    bindings:
      - { path: /webhook/orders, mode: proxy }        # 缺省即 proxy（决策 D3）
      - { path: /tasks/orders, mode: bus, topic: orders.events }  # 显式异步
```

- **实例状态机**（actual）：`starting → running ⇄ paused → stopping → stopped`；异常 → `failed → restarting → starting`（受 policy 约束）；失联 → `unknown`。
- **对账模型**：plane 存期望状态（唯一来源），agent 每心跳 desired/actual 对账并执行差异（声明式，命令丢失后仍收敛）。
- **约束**：本版 1 实例 = 1 flow = 1 进程；单机多 flow 延后。

## 4. 调度（Gateway 路由）

**模式语义（决策 D3）**：

| 模式 | 语义 | 适用 |
|---|---|---|
| `proxy`（**默认**，未声明 mode 即此） | 反向代理到所选实例的 http 触发器（agent 注册实例端口） | 同步请求-响应（webhook） |
| `bus`（显式 `mode: bus`） | 发布到总线 topic，订阅实例竞争消费；**发布即返回，无响应回执** | 异步任务/作业队列 |

**选择策略**：

- `round-robin`：轮转（默认，D3 即有）。
- `hash`：对 `sticky_key` 一致性哈希；**弱 sticky**——实例增减时允许漂移，重试可落到别的实例；正确性靠业务幂等键（文档化，不承诺「同一键必须同一实例」）。
- `tags`：标签/权重亲和 + 过滤（D3 即有）。
- `least-loaded`：按队列深度/CPU/错误率加权——**依赖 D1 的队列深度指标，D5 之后启用**。

**失败处理**：`unknown/failed/stopped` 实例摘除；proxy 请求级重试（幂等安全）；全挂 503 + 告警。

## 5. 生命周期与监控

- **健康检查**：agent 本地 1s 探测（轻量）；连续失败 N 次（policy.health_failures）→ 标记 unhealthy → 按策略重启（指数退避，超 `max_restarts` 熔断为 failed + 告警）。plane 基于 5s 心跳收敛决策，允许秒级滞后（快故障由 agent 本地兜底）。
- **重启恢复语义（进程级管理的连续性契约）**：
  - agent 重启实例时以 **resume 语义**启动（复用 store 中的活跃 run_id 与 delivered 键；D2 给单机 CLI 增加 `serve --resume`/等价支持）。
  - `restart: always` 的实例**必须声明 checkpoint store**，否则策略降级为「只告警不重启」，避免静默重复投递。
  - 文档化幂等边界：内容哈希 delivered 键随 run_id 变化，**跨重启去重依赖业务幂等键**（metadata key）或上游幂等。
- **监控决策**：内置规则（连续健康失败 → restart）+ 阈值告警（队列深度/错误率 → Dashboard 高亮）；自动扩容不在本版。
- **手动操作**：start/stop/restart/pause/resume/approve（映射单机 PauseGate 与 admin API）。

## 6. 单机 Dashboard（serve 模式，需求 5）

**扩展现有 admin API**（现在只有 health/metrics/pause/resume）：

| 端点 | 内容 |
|---|---|
| `GET /admin/status` | flow 元信息、triggers 列表、步骤 + 实时 StepStat、paused、run_id、启动时间、approve 队列概要 |
| `GET /admin/history` | 本实例运行历史（run 记录 + **serve 停止时也写一条**，D1 补齐） |
| `GET /admin/approvals` | approve 停放队列列表（步骤 + 数量 + 样例） |
| `GET /admin/errors` | 近期错误环形缓冲（executor 打点处新增） |
| `--admin-token` | 管理端点鉴权（D1；agent 与 plane 使用） |

**`GET /admin/ui`**：内嵌静态单页（vanilla JS/htmx，`rust-embed` 打进二进制，无前端构建），页面：

- 工作状态总览：flow、run_id、启停时间、processed/filtered/errors/waiting 实时卡片。
- 任务详情：DAG 视图（步骤节点 + 路由边）、每步骤统计/耗时表、paused 状态与 pause/resume 按钮。
- 触发器面板：http/cron/queue 清单与最近活动。
- 审批面板：approve 队列 + 审批操作。
- 错误与历史：近期错误流、本机 run history 列表。

数据走上述 JSON API，UI 2–5s 轮询。`--admin` 未开启时 UI 不可用（保持默认零暴露）。

## 7. 全局 Dashboard（Manager UI）

同单机 UI 技术栈，静态资源内嵌 plane 二进制。页面：

- **总览**：节点/实例健康矩阵、集群吞吐与错误率、告警列表。
- **实例详情**：聚合单机 status/metrics（plane 缓存心跳快照 + 按需直查实例），生命周期操作按钮。
- **调度配置**：路由表/策略编辑（复用 sohara-config 校验）。
- **部署**：上传/编辑 flow → 分发节点 → 创建/更新实例。
- **运行历史**：跨实例聚合（agent 上报单机 history），下钻单次运行与单实例。

## 8. 实施路线（D 阶段，每阶段可独立验收）

| 阶段 | 内容 | 验收 |
|---|---|---|
| D1 ✅ | 单机 Dashboard：admin API 扩展（status/history/approvals/errors/错误环形缓冲/`--admin-token`）+ 内嵌 `/admin/ui`；serve 停止写 history；CLI `serve --resume` | 已实现：`sohara serve --admin` 打开 UI；status/errors/approvals/history 端点可用；无 token 401；暂停期间事件不处理；停机写 history |
| D2 ✅ | `sohara-agent`：进程管理（spawn/kill/重启退避）、本地 1s 健康检查、心跳上报、命令执行、token；单机 `serve --resume` | 已实现：`sohara-agent` crate（实例监督状态机/重启策略/HttpTransport 心跳+命令队列+seq 去重）；e2e 用真实 `sohara` 二进制验证拉起/健康/停机；plane stub 测试验证心跳与 pause 命令执行 |
| D3 | `sohara-plane` 基础：Registry、Manager API、期望状态下发、desired/actual 对账；路由策略 round-robin/hash/tags | Manager API 声明实例 → agent 拉起/停止真实 sohara 进程 |
| D4 | Gateway + 调度：路由表、proxy 默认模式、健康摘除、请求级重试 | 两个实例按策略分布流量；停一个实例流量自动切走 |
| D5a | PlaneRelayBus：跨机 queue 发布/订阅（agent 转发、有界积压+丢弃告警） | A 机发布 → B 机 queue 流程消费落盘；B 机离线时积压/丢弃按预期 |
| D5b | （可选，后期）NATS/JetStream：`NatsBus` + `nats` 触发器；least-loaded 策略启用 | 跨机持久化投递；重启不丢消息 |
| D6 | 全局 Dashboard + 安全收尾（mTLS 可选）+ 文档 + Gateway 前置 LB 评估 | Manager UI 完成 §7 页面；三向 token 全链路生效 |

## 9. 失败模型与安全

- **agent 失联**：plane 标记 unknown → Gateway 摘除 → 告警；恢复后对账。
- **plane 宕机**：agent 策略 `keep-running`（保持现状运行），命令队列重连补拉；期间 Gateway 不可用（单点，明确接受，HA 延后；D6 评估前置 LB）。
- **中转积压**：plane 每订阅实例有界队列，超限丢弃 + 告警（对齐单机有界通道语义）；持久化/至少一次由 D5b NATS 承接。
- **消息幂等**：run 内由 delivered 键保证；跨重启依赖业务幂等键（§5 契约）；D5b 的 JetStream 至少一次 + 消重同契约。
- **安全**：plane/agent/实例三向 token（D1/D2 落地）；Dashboard 管理操作需权限；单机 admin UI 默认仅 `--admin` 显式开启，建议绑定 loopback/内网。

## 10. 与现有代码的接缝（零破坏）

| 现有 | 用途 |
|---|---|
| `EventBus` trait（core） | D5a `PlaneRelayBus`、D5b `NatsBus`（同 trait，单机 `InProcessBus` 不动） |
| `Trigger` trait + queue 触发器模式 | D5b `nats` 触发器（复用有界通道/背压/优雅停机）；D5a 复用既有 queue 触发器 |
| admin API（S6） | agent 健康检查/指标采集；D1 扩展为 Dashboard 数据源 |
| `RunReport`/`StepStat`（S6） | 心跳指标快照 → Scheduler 决策 |
| run history 文件 | D1 补 serve 停止历史；agent 上报 → 全局聚合 |
| `PauseGate`（S6） | plane pause/resume 命令落点 |
| release 二进制 `sohara serve --admin` | agent 进程级管理（决策 D2）；单机 CLI 仅增 `--admin-token`/`--resume` 两个 flag |

**新增 crate**：`sohara-agent`、`sohara-plane`、`sohara-dashboard`（UI 资源，可并入前两者）；D5b 时增 `sohara-nats`。
