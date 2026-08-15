# Sohara 重新设计与多阶段增量规划

> 状态：已采纳（accepted，含 challenge 评审修订）
> 目标：把 [tiger](../../../../fe/tiger)（事件驱动模块框架）与 [rec-core](../../../../ml/rec-core)（流式数据处理框架）的概念合并为一套统一的单机自动化数据/流程工作流框架，并给出从最小可验证 MVP 到完善自动化框架的增量路线。
>
> 本文是对 `docs/design/draft/yaml-defiition.md` 中草稿与开放问题的正式回答与落地。

> **评审修订结论（challenge）**：
> 1. **单机优先，不做分布式** —— 多节点能力不在本路线图，未来以更高层抽象（外部队列/调度器）承载。
> 2. **AI agent 为未来扩展，不在本范围实现**。
> 3. **S0–S4 采用无 schema JSON 单路径** —— `Schema/DataType/DataFrame` 不进入核心，作为 S5 可选增强。
> 地基决策见 §2.3（数据模型）、§2.4（投递/控制流/错误/并发）、§2.5（投递语义）。

---

## 目录

1. [现状与概念映射](#1-现状与概念映射)
2. [重新设计](#2-重新设计)
3. [多阶段增量路线图](#3-多阶段增量路线图)
4. [对草案开放问题的回答索引](#4-对草案开放问题的回答索引)

---

## 1. 现状与概念映射

### 1.1 三个项目的定位

| 项目 | 语言 | 定位 | 核心范式 |
|---|---|---|---|
| `tiger` | TypeScript | 极简事件驱动服务器（webhook/cron/queue） | **有状态模块 + 协议解析器** |
| `rec-core` | Java | 数据记录文件的校验与转换框架 | **函数式流式管线（Source→Tee→Target）** |
| `sohara`（现状） | Rust | 轻量事件驱动数据处理框架（早期） | Source/Sink/Transform/Pipeline + Record（JSON） |

三者本质都在回答同一组问题：**数据从哪来（源/触发）、如何被加工（变换）、到哪去（输出）、如何编排（流程）、如何记住状态（状态/持久化）、如何被扩展（插件）**。tiger 侧重「事件驱动的流程编排」，rec-core 侧重「数据流的类型化加工」，二者互补而非互斥，可以合并。

### 1.2 tiger 的核心概念（已读源码归纳）

- **Module（模块）**：原子单元 `{ id, target: "protocol:path", process(state, param) -> Partial<State>, distributed }`。有状态、事件驱动；`process` 返回的局部状态会被合并进模块状态。
- **Plugin / Resolver（协议解析器）**：把 `protocol:path` 字符串绑定到「定义（`define`，注册模块到目标）」与「派发（`notified`，把消息投递给模块）」两个动作。这是一套**可插拔 I/O / 触发抽象**。
- **notify(target, param)**：模块间消息传递，等价于流程图的边。
- **State（模块状态）**：每模块一份，`distributed` 模式下持久化到存储。
- **PersistenceProvider**（LevelDB / Postgres）：节点心跳注册表、模块状态、任务队列（`claim/ack/fail/requeue`）、任务历史、cron 调度。
- **DistributedCoordinator**：共享队列 + 状态 + 心跳（超时重派发）+ 节点启停 + 队列上限背压（`maxQueueLength`）。
- **Monitor / Management**：运行历史记录 + 管理面板（`/tiger/manage`），支持暂停/恢复节点消费。
- **Config**：http/cron/monitor/distributed 均可用环境变量覆盖。

### 1.3 rec-core 的核心概念（已读源码归纳）

- **Source → Tee → Target**：拉取式函数式管线（`Source.stream() -> Stream<DataSet>`）。
- **DataSet / Schema / DataType / DataFrame**：类型化表格数据模型（列名 + 列类型 + null bitmap；列式存储 `ColumnVector`）。
- **内置实现**：CSV 源/目标、`CollectTee`、`ItemCounterTee`、`TransformTee`、`stateful`（累加器）、`unique`、`cache`（二进制回放）、`ReactiveTee`（推式链）。
- **数据格式插件**：jsonl、parquet、jdbi(DB)、reactive，均通过 `RecPlugin`（Java `ServiceLoader`）自动发现，暴露为 JS 命名空间 `__<name>`。
- **Scripting**：Rhino JS + `require("rec")`；`rec.file/csv/action/pred/stateful/flat/target/dummy/counter/collect/cache/println`。
- **ExecutionContext / RestartableSource**：计数级 checkpoint，`commit/persist/restart`，`.retry` 文件恢复。
- **Agent（可选）**：OpenAI 工具（Read/Write/Edit/Glob/Grep/WebFetch/Bash/ExecuteRecScript）、子代理（Plan/Explore）、MCP(stdio)、skills、AGENTS.md。

### 1.4 概念映射（合并结论）

| tiger | rec-core | Sohara（统一后） |
|---|---|---|
| Module | Source / Tee / Target | **Step**（统一节点，按 `kind` 区分角色） |
| protocol resolver（define/notified） | 源/目标工厂 | **ComponentRegistry + Trigger/Source/Sink trait** |
| `notify(target, param)` | 管线边（tee/`.to()`） | **Flow 图的 Edge** |
| Module state | `stateful` tee / ExecutionContext | **StepState + ExecutionContext（checkpoint/resume）** |
| PersistenceProvider + 任务队列 | `cache` 回放 + `.retry` 文件 | **StateStore（状态/历史/队列/检查点）** |
| queue 插件（进程内总线） | ReactiveTee（推式链） | **事件总线（有界 channel/队列）** |
| 无 schema（消息即 JSON） | Schema / DataType | **无 schema JSON 单路径（S0–S4）；Schema 作为 S5 可选增强** |
| `serve` 常驻 | `run` 脚本（一次性） | **双模式：`sohara serve` / `sohara run`** |

**明确收敛的决策**：

1. tiger 的 `distributed` **不在本路线图实现**（单机优先）；仅保留「部署维度」扩展点，未来以更高层抽象（外部队列/调度器）承载，不把多节点协议污染进单机模型。
2. rec-core 的 `DataFrame`/`Schema`/`DataType` **不进入核心路径**：S0–S4 只做无 schema JSON 单路径，`Schema` 作为 S5 可选增强（见 §2.3）。
3. 两套 JS 脚本（Rhino / QuickJS）统一为 **QuickJS**（沿用 `Cargo.toml` 已声明的 `quick-js`），只保留一套脚本桥；MVP 采用**同步宿主调用**（学 rec 的 Rhino 全同步），异步桥接为后续可选增强（见 `quickjs-api.md` §7）。
4. tiger 的「协议字符串 `protocol:path`」保留其**命名空间思想**（`kind:type`），但不复用其字符串解析约定；Sohara 使用结构化的 `kind` + `type` 字段，便于 YAML 与校验。
5. rec-core 的 `ReactiveTee` 推式语义由「运行时调度器 + 有界 channel 事件总线」统一承载，不再单独成为一种用户可见的管线种类。

---

## 2. 重新设计

### 2.1 统一概念模型

- **Flow（流程）**：一张有向图，由 **Step（步骤）** 与 **Edge（边）** 构成；`Record` 沿边流动。这是 tiger「模块图 + notify」与 rec-core「Source→Tee→Target」的统一。
- **Step**：`{ id, kind, config }`，`kind ∈ { source, transform, sink, control }`：
  - `source`：http / cron / queue / file / manual（`db-subscription`/`push` 为未来扩展）。
  - `transform`：map / filter / assert / aggregate / merge / join / split / script / batch。
  - `sink`：file / db / http / queue / log / noop / email。
  - `control`：switch（分支）、foreach / loop（循环）、parallel + join（并发/扇出汇聚）、wait / approve（人工在环）。
- **Trigger（触发器）**：带 `start()/stop()` 生命周期的 `source`，供 `serve` 模式常驻运行。
- **Record（记录）**：`{ id, timestamp, payload, metadata }`；`payload` 为 **JSON 值（`serde_json::Value`）单路径**（见 §2.3）。
- **ExecutionContext（执行上下文）**：一次运行 / 一次事件触发所共享的上下文，承载 `StateStore` 引用、checkpoint、correlation id、取消信号与指标计数。
- **ComponentRegistry（组件注册表）**：`(kind, type)` → 工厂；内置组件默认注册，Rust 库（编译期）与 QuickJS 脚本（运行期）扩展。对应 tiger 的 resolver 注册表与 rec 的 `ServiceLoader`。

### 2.2 crate 布局（在现有 workspace 上增量演进）

> 现有 workspace 仅含 `sohara-core`；`Cargo.toml` 已声明 `tokio / axum / cron / sqlx / quick-js / crossbeam-channel / tokio-stream / chrono / uuid / dashmap` 等依赖，恰好预示了下述各 crate 的分工，将在对应阶段接入。

```
sohara/
├── sohara-core        # 数据模型(Record[serde_json]) + Step/Source/Transform/Sink trait + Error + Registry
├── sohara-runtime     # Flow 图、调度器(顺序/并发/批量)、ExecutionContext、StateStore trait、事件总线、优雅停机
├── sohara-config      # YAML/serde schema、校验、加载、imports
├── sohara-builtins    # 内置步骤：filter/map/aggregate/merge/assert、file/log/vec 等
├── sohara-io          # 数据格式与连接器：csv/json/jsonl/parquet、db(sqlx)、http 客户端
├── sohara-triggers    # http(axum)、cron、queue/事件总线
├── sohara-js          # QuickJS 脚本桥 + script 步骤
├── sohara-cli         # `sohara init` / `run <flow.yaml>` / `serve <flow.yaml>`
├── sohara-persistence # StateStore 实现：memory/rocksdb/sqlite（单机）
└── sohara-server      # 运行历史/指标/管理 API（单机）
```

> `sohara-agent` 与分布式（多节点）能力为**未来扩展，不在本路线图**。

**crate 与阶段对应**：

| crate | 引入阶段 |
|---|---|
| `sohara-core` | S0（在现有基础上收敛数据模型与 trait） |
| `sohara-config`、`sohara-builtins`、`sohara-cli` | S1 |
| `sohara-runtime` | S2（图与调度） |
| `sohara-triggers` | S3 |
| `sohara-persistence` | S4 |
| `sohara-io`、`sohara-js` | S5 |
| `sohara-server` | S6 |

### 2.3 数据模型

> **地基决策（定稿）**：S0–S4 采用**无 schema JSON 单路径**；rec-core 的 `Schema/DataType/DataFrame` 不进入核心，作为 S5 可选增强（届时以「`Schema` 可选字段 + 校验层」方式引入，不替代、不破坏 JSON 路径与既有 API）。

#### Record（单路径 JSON）

沿用现状签名，`payload` 保持 JSON 值，`id/timestamp/metadata` 不变——从而 `Record::from_json(...)`、`record.set(field, serde_json::Value)` 与 README Quick Start 全部无需重写：

```rust
pub struct Record {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub payload: serde_json::Value,   // S0–S4 唯一路径
    pub metadata: HashMap<String, String>,
}
```

- 字段访问：`get/set` 支持点路径（`a.b`）；缺失返回 `None`，`set` 自动建中间对象。
- S5 可选增强：引入 `Schema/DataType` 作为 `Record` 的可选字段 + 校验层，不改变上述 JSON 主路径。

#### TransformOutcome

修复现有 `transform.rs` 中「用 `Error::Transform` 表示被过滤」的语义缺陷，并为错误显式建模：

```rust
pub enum TransformOutcome {
    Pass(Record),            // 继续流动
    Filtered,                // 被过滤，停止（计入 filtered）
    Expand(Vec<Record>),     // 一对多（split / flat-map）
    Fail(Error),             // 步骤失败（计入 errors，按 on_error 策略处理）
}
```

### 2.4 执行模型

- **`run`（一次性）**：加载 YAML → 校验 → 构图 → 拓扑调度 → 流式执行 → flush → 输出统计后退出。对应 rec-core 的脚本运行方式。
- **`serve`（常驻）**：启动 `triggers` 声明的触发器（http/cron/queue），事件进入时构造 `Record` 并注入图；监听信号优雅停机。对应 tiger 的 `serve()`。
- **投递抽象（定稿）**：统一为拉取式异步 `Stream<Record>`（沿用 `sohara-core` 的 `BoxStream`）；所有推送事件源（http/cron/queue）通过**有界 `mpsc::channel`** 桥接为 `Stream`；**背压 = 通道容量**（对应 tiger 的 `maxQueueLength`）。`run` 耗尽即退出，`serve` 常驻。
- **调度语义**：
  - 顺序（默认）：单条记录沿拓扑逐级流转。
  - 并发：算子级并发 + 有界 channel 背压。
  - 批量：按「N 条」或「时间窗口」聚合后向下游投递。
  - 级联（cascaded）：多段子流程顺序串联（`A → B → C`）；S2 用线性 `edges` 表达，S5 用 `subflow` 步骤嵌套表达。
- **控制步骤语义（定稿）**：`switch/foreach/loop/parallel/join` 是**运行时原语**；其分支/循环体作为「下游子图」迭代/嵌套执行，**不构成主图上的环**——主图仍要求 DAG。`loop.while` 基于 `ctx.state` 求值。
- **错误语义（定稿）**：步骤失败 → `TransformOutcome::Fail` / Sink 错误，按 `on_error: fail | continue | retry` 处理（默认 `fail`）；`on_error: retry` 时用 `retry: {max, backoff, on}` 参数；且**一律计入 `stats.errors`**（修复现状 `pipeline.rs` 吞 transform 错误不计数的问题）。
- **状态并发（定稿）**：`ctx.state` 由运行时按步骤实例**串行化**（单写者/互斥），脚本无需自行加锁；并行扇出下不产生竞态。

### 2.5 状态与恢复（长流程）

> **决策**：单机优先，不做分布式；以下 `StateStore` 均为单机实现（memory/rocksdb/sqlite）。

统一 tiger 的持久化队列语义与 rec 的 checkpoint 语义，形成一套**单机 at-least-once + 幂等去重**模型：

```
StateStore trait:
  step_state    // 步骤累加状态（tiger module state / rec stateful）
  run_history   // 运行历史（tiger monitor recordRun）
  job_queue     // claim/ack/fail/requeue（崩溃恢复）
  cron_schedule // cron 下次运行持久化
  checkpoint    // 源位置 + 步骤状态 + 未决任务
```

- `commit/persist/restart`：计数或时间间隔触发 checkpoint；`sohara run --resume` 或 `.retry` 文件恢复。
- **投递语义（定稿）**：at-least-once；配合可选**幂等键**（record id）在 sink 去重，达成「不丢、可去重」。
- `wait / approve`（人工在环）：流程进入 `WAITING` 状态并持久化，经 CLI/API 批准或退回后继续。

### 2.6 插件 / 组件注册表

```
ComponentRegistry:
  register(kind, type, factory)
  resolve(kind, type) -> Step
```

- 内置组件：`filter / map / aggregate / merge / assert / file / log / vec / http / cron / queue` 等。
- 扩展途径：Rust 库（编译期）、QuickJS 脚本（运行期 `script` 步骤与 `sohara.registerStep`，见 [`quickjs-api.md`](quickjs-api.md)）、未来 WASM。
- 命名空间：`(kind, type)` 取代 tiger 的 `protocol:path` 与 rec 的 `__plugin`，统一且可校验。

### 2.7 YAML schema（分阶段演进）

> 完整权威 schema（顶层字段、全部 step 类型与 config、edges、表达式、imports、triggers、checkpoint、校验规则、版本演进）见 [`yaml-workflow-schema.md`](yaml-workflow-schema.md)。

#### S1（线性形态，最小可用）

```yaml
name: example
version: "1"
steps:
  - { id: in,    kind: source,    type: file,      format: csv,   path: data.csv, columns: [name, age] }
  - { id: adult, kind: transform, type: filter,    where: "age > 18" }
  - { id: out,   kind: sink,      type: file,      format: jsonl, path: out.jsonl }
edges: [[in, adult], [adult, out]]
```

省略 `edges` 时，仅当**有且只有一个 source/trigger** 才允许线性串联；否则必须显式写 `edges`（见 schema §10）。

#### S2（图 + 控制流）

```yaml
steps:
  - { id: fanout, kind: control, type: parallel, branches: [a, b] }
  - { id: branch, kind: control, type: switch, cases: [{ when: "amount > 1000", to: big }], default: small }
  - { id: loop,   kind: control, type: foreach, over: "$.items", as: item }
  - { id: batch,  kind: transform, type: batch, size: 100, within: 5s }
```

#### S3（触发器）

```yaml
triggers:
  - { id: webhook, type: http, method: POST, path: /webhook }
  - { id: tick,    type: cron, expression: "*/5 * * * * *" }
  - { id: bus,     type: queue, topic: hello }
steps:
  - { id: handle, kind: transform, type: map, ... }
  - { id: sink,   kind: sink, type: log }
edges: [[webhook, handle], [tick, handle], [bus, handle], [handle, sink]]
```

#### S4（状态与恢复）

```yaml
checkpoint: { every: 1000 }      # 计数 checkpoint（顶层）
steps:
  - { id: count, kind: transform, type: map, state: { count: 0 },
      on_error: retry, retry: { max: 3, backoff: 1s } }   # 步骤级状态 + 错误策略
  - { id: approve, kind: control, type: approve, config: { title: "请审批", owners: [alice] } }
```

#### S5（扩展与复用）

```yaml
imports: [common-steps.yaml]     # YAML 片段复用（答复「yaml 导入」）
steps:
  - { id: enrich, kind: transform, type: script, script: enrich.js }   # QuickJS
  - { id: from_db, kind: source, type: db, query: "SELECT ..." }
  - { id: to_parquet, kind: sink, type: parquet, path: out.parquet }
```

### 2.8 表达式语言

- S1 阶段用最小谓词（字段比较）即可，`where: "age > 18"`。
- S2 起引入一个轻量表达式/路径子集（JSON Path 风格 `$.items`），完整语法（含优先级/类型强转）见 [`yaml-workflow-schema.md`](yaml-workflow-schema.md) §6。
- 复杂逻辑一律交给 `script` 步骤（QuickJS，完整 API 见 [`quickjs-api.md`](quickjs-api.md)），不在 YAML 内实现图灵完备语言——保持声明式「声明 + 可读」，命令式「下沉到脚本」。

---

## 3. 多阶段增量路线图

> 原则：每个阶段**可独立交付、可独立验证**，阶段间单向依赖 `S(n) 依赖 S(n-1)`。验收统一以「`cargo test`（含集成测试）+ 一个可复现 example + 文档同步」为准。**AI agent 与分布式明确不在本路线图**，S6 之后为完善与扩展点预留。

### S0 — 核心 MVP：JSON Record + 线性管线

- **目标**：在现有 `sohara-core` 上证明「JSON 记录 + 线性管线」端到端可跑通，且错误统计正确。
- **交付**：
  - `Record`（`serde_json::Value` 单路径，`get/set` 点路径）+ `Error`/`Result` 收敛。
  - `Source` / `Transform`（返回 `TransformOutcome`）/ `Sink` trait 收敛。
  - `Pipeline`（线性）与 `PipelineStats`（processed/filtered/errors 计数正确）。
  - 内置 `VecSource`、`VecSink`、`LogSink`、`MapTransform`、`FilterTransform`、`AddFieldTransform`、`AssertTransform`。
  - 单元测试 + `examples/basic.rs`。
- **验收标准**：`cargo test` 全绿；example 打印 `processed/filtered/errors` 统计正确；transform 失败计入 errors（不吞错）。
- **范围外**：YAML、CLI、图、触发器、持久化、Schema。

### S1 — 声明式 + CLI（run 模式）

- **目标**：用 YAML 声明式驱动一次性管线，提供 `sohara run`。
- **交付**：
  - `sohara-config`：YAML schema v1（线性 flow）+ 校验 + 友好错误。
  - `sohara-builtins`：文件/日志/过滤/map 等内置步骤。
  - `sohara-cli`：`sohara init`（脚手架）与 `sohara run <flow.yaml>`。
  - `ComponentRegistry`（`(kind, type)` → 工厂）。
- **验收标准**：`sohara run examples/basic.yaml` 产出正确结果；非法配置给出可读错误；`cargo test` 覆盖配置校验。
- **范围外**：控制流、触发器、持久化。

### S2 — 流程 DAG + 并发

- **目标**：从线性升级为有向图，支持控制流与并发。
- **交付**：
  - `sohara-runtime`：`FlowGraph`、拓扑调度、执行上下文雏形。
  - 控制步骤：`switch`、`foreach`/`loop`、`parallel` + `join`、`batch`（循环体为下游子图迭代，不构成主图环）。
  - per-step 错误策略（`fail/continue/retry`）。
- **验收标准**：分支/循环/并发/批量四类集成测试通过；`cargo test` 全绿。
- **范围外**：触发器、持久化、脚本。

### S3 — 触发器 + serve 模式

- **目标**：事件驱动常驻运行，打通「webhook/cron/queue → 管线 → sink」。
- **交付**：
  - `sohara-triggers`：http(axum)、cron、queue（进程内事件总线，有界 channel 背压）。
  - 运行时事件循环 + 优雅停机（信号处理）。
  - 事件 → `Record` → 图入口映射。
- **验收标准**：`sohara serve` 跑通 webhook→管线→sink；cron 按时触发；queue 消息可达；`Ctrl+C` 优雅退出。
- **范围外**：状态持久化、checkpoint、多节点。

### S4 — 持久化 + 恢复 + 人工在环

- **目标**：长流程可记录、可恢复，支撑企业级审批。
- **交付**：
  - `sohara-persistence`：`StateStore`（memory/rocksdb/sqlite，单机）。
  - checkpoint（计数/时间）与 `--resume` / `.retry`。
  - `wait`/`approve` 步骤（`WAITING` 状态持久化，CLI/API 恢复）。
- **验收标准**：运行中 kill 后 resume「不丢、可去重（at-least-once + 幂等键）」；approve 暂停后可恢复；状态跨重启存活。
- **范围外**：分布式、监控面板、连接器扩展。

### S5 — 连接器 + 脚本

- **目标**：丰富 I/O 与扩展性，进入「自动化框架」形态。
- **交付**：
  - `sohara-io`：csv/json/jsonl/parquet、db(sqlx)、http 客户端。
  - `sohara-js`：QuickJS 脚本桥（同步宿主调用）+ `script` 步骤。
  - `imports` YAML 片段复用。
  - 可选：`Schema/DataType` 作为 Record 校验层增强（不破坏 JSON 主路径）。
- **验收标准**：csv→parquet 示例、db 源→目标、`script` 步骤、`imports` 复用均跑通。
- **范围外**：监控面板、多节点、AI、异步脚本桥接（可选 spike）。

### S6 — 可观测 + 管理（单机）

- **目标**：单机监控与管理（对齐 tiger 的 monitor；不含分布式）。
- **交付**：
  - 运行历史：每次 `run` 追加一条 JSONL 记录（run_id/flow/起止时间/status/统计/步骤统计）到 `.sohara/history.jsonl`（`--history` 可改）；`sohara history [--limit N]` 查看；失败的运行也记录（status=error）。
  - 指标：执行器记录每步骤 `{processed, filtered, errors, nanos}`（`RunReport`）；`run --verbose` 打印步骤统计表；serve 模式 `/admin/metrics` 返回 JSON 报告。
  - 管理 API（serve 模式，`--admin ADDR`）：`GET /admin/health`、`POST /admin/pause`、`POST /admin/resume`、`GET /admin/metrics`。**暂停语义（定稿）**：协作式暂停——执行器暂停后把已拉取的记录「握在手里」不处理、也不再拉取下一条（背压自然向上游传播），恢复后继续。
  - 实现位置说明：管理 API/历史内置于 runtime + CLI，未单独建 `sohara-server` crate（避免与 runtime 循环依赖；单机场景收益不明显）。
- **验收标准**：`sohara history` 展示运行历史（含失败运行）；`--admin` 的 pause/resume 生效（暂停期间新事件不被处理）；`--verbose` 展示步骤统计。
- **范围外**：多节点、AI 代理、Prometheus 文本导出、步骤/触发器级 disable（延后）。

### S7 — 完善 + 打包（扩展点预留）

- **目标**：文档、示例、CI、打包发布；预留扩展点。
- **交付**：
  - 示例库（每阶段一个可复现示例 + `examples/README.md` 索引）、CI（`.github/workflows/ci.yml`：fmt/clippy(-D warnings)/test/长度门禁）、发布打包（release profile：thin LTO + strip，`cargo build --release -p sohara-cli` 产出可分发二进制）。
  - README 重写为实装状态（CLI 命令表、示例表、crate 布局、开发命令）。
  - 扩展点预留：`docs/design/extension-points.md`（脚本插件接口、高层分布式接口、AI agent 接口——仅接口/文档，不实现）。
- **验收标准**：文档/示例/CI 完整；`sohara` 可发布为可分发二进制。
- **范围外**：分布式实现、AI agent 实现。

---

## 4. 对草案开放问题的回答索引

| `yaml-defiition.md` 的问题 | 本文回答位置 |
|---|---|
| trigger / source / transform / sink 这一套 | §2.1（`Step.kind` 统一）；§2.7 YAML |
| sequential / concurrent / cascaded / batch 这一套 | §2.4（调度语义：顺序/并发/级联/批量）；§2.7 S2；路线图 S2 |
| Rescenario 场景（capture、transform、assertion/expect） | `assert` 归入 `transform`（S0 提供 `AssertTransform`）；「场景」即一个带 `assert` 的 Flow |
| `sohara serve` / `sohara run` | §2.4；S1（run）、S3（serve） |
| yaml 文件互相导入共享 step | §2.7 S5（`imports`）；S5 实施 |
| 执行可记录、可恢复、流程可持久化（长时流程） | §2.5（StateStore + checkpoint）；S4 实施 |
| 条件定义（分支/循环/复杂业务流） | §2.7 S2（`switch/foreach/loop`）；S2 实施 |
| human-in-the-loop（approve/退回） | §2.5（`wait/approve`）；S4 实施 |

---

## 附：术语对照（中 / 英）

| 中文 | 英文 |
|---|---|
| 流程 | Flow |
| 步骤 / 节点 | Step / Node |
| 边 | Edge |
| 源 / 触发器 | Source / Trigger |
| 变换 | Transform |
| 输出 / 汇聚 | Sink / Target |
| 记录 | Record |
| 模式 / 表结构 | Schema |
| 状态 | State |
| 执行上下文 | ExecutionContext |
| 检查点 / 恢复 | Checkpoint / Resume |
| 组件注册表 | ComponentRegistry |
| 状态存储 | StateStore |
| 人工在环 | Human-in-the-loop |
