# Sohara Extension Points (S7 预留，不实现)

本文记录为未来预留的扩展接口与方向。**当前路线图明确不在范围**：分布式实现与 AI agent 实现（见 `redesign-and-roadmap.md` §1、§3）。这些接口仅作为设计锚点，避免未来破坏性改动。

## 1. 脚本插件接口（自定义步骤）

现状：`sohara-js` 提供 `source.script` / `transform.script` / `sink.script`，脚本每次调用创建隔离的 QuickJS context。

预留（对齐 quickjs-api §8 `sohara.registerStep`）：

- 脚本内注册 `(kind, type, handler)` 到 `ComponentRegistry`，使纯脚本步骤与 Rust 步骤同权。
- 需要解决的问题（届时定稿）：
  - 跨调用上下文复用与状态隔离（per-invocation context vs 常驻 context 的选择）。
  - `handler` 的调用约定：与 `transform(record, ctx)` / `consume(record, ctx)` 对齐，返回 `TransformOutcome` 语义（record / null / record[] / throw）。
  - 注册生命周期：流程加载期（声明式）还是运行期（动态）。

## 2. 高层分布式接口

> **已进入正式设计**：分布式管理层（控制多单机实例、调度、生命周期、Gateway/Manager、两级 Dashboard）的完整设计见 [`distributed-plane-and-dashboard.md`](distributed-plane-and-dashboard.md)（v2，含 challenge 决策记录与 D1–D6 实施路线）。本节保留接口层面的锚点说明。

现状：单机为主（用户定稿：单机优先，不做分布式）。事件总线 `EventBus` trait 与有界 channel 背压语义已为将来替换传输层留好形状。

预留：

- `EventBus` 的网络实现（NATS / Redis Streams / Kafka 等）：`QueueSource`/`QueueSink` 与触发器只依赖 trait，替换传输层不触碰步骤代码。
- `StateStore` 的共享存储实现（数据库 / 对象存储）：checkpoint 与幂等投递键目前是单机文件/内存语义，迁移共享存储需要**分区与锁**（同 run_id 并发安全）。
- 执行器分片：`parallel` 控制步骤的跨节点扇出（correlation id 已存在于记录路径，是自然的分片键）。

原则：先跑通单机正确性（幂等投递、checkpoint、审批队列），再考虑「多实例 + 共享存储」的横向扩展；接口不提前加分布式假设。

## 3. AI agent 接口

现状：不在路线图。设计锚点（对齐 tiger 的 agent 概念）：

- Agent = 携带工具（tool）与记忆（memory）的**特殊脚本步骤**，宿主桥注入 `sohara.llm.*` 调用：
  - `sohara.llm.complete(prompt, opts)` / `sohara.llm.chat(messages, opts)`：模型调用（provider/model/temperature 等 opts）。
  - 工具以函数签名暴露（结构化输出约束 → 校验 → 错误回传）。
- 运行保障（届时必做）：调用预算（token/次/时长）、超时与重试、审批联动（`approve` 步骤复核 agent 输出）、审计日志（prompt/response 落运行历史）。
- 明确不做：agent 自治编排（多 agent 协作、目标循环）——单机框架只提供「LLM 调用 + 工具桥」原语，编排仍由声明式 flow 表达。

## 4. 其他候选扩展（排序待定）

| 方向 | 说明 | 依赖 |
|---|---|---|
| Schema/DataType 增强 | `Record` 可选 schema + 校验层（S5 已裁剪） | 用户定稿：不进入 JSON 主路径 |
| parquet / text 格式 | `file` 源与汇的格式扩展 | 可选依赖 arrow/parquet |
| Prometheus 文本指标 | `/admin/metrics` 增加 text/plain 输出 | 单机可观测性（S6 裁剪） |
| 步骤/触发器级 disable | 管理 API 的细粒度开关 | admin API 状态模型扩展 |
| `sohara-server` 独立服务 | 面板/运行历史 UI | 目前内置于 runtime + CLI |
| email 汇 / join 变换 / subflow | S5 计划中裁剪的类型 | 各类型工厂 + 测试 |
