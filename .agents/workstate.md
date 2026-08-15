# Workstate

## 当前焦点
- 分布式管理层实施中（目标 goal-86bd2ea3）：**D1 ✅、D2 ✅、D3 ✅、D4 ✅**，下一步 **D5a PlaneRelayBus（跨机事件总线）**。

## 状态 / 阻塞
- 无阻塞。116 测试全绿、clippy 0、fmt、长度门禁；真实三进程 e2e 验证 gateway 调度与摘除。
- 沙箱环境约束：cargo 需 `CARGO_HOME=/Users/qliu23/workspace/fe/sohara/.cargo-home`；本机 HTTP_PROXY=127.0.0.1:7890，本地 HTTP 需 no_proxy（已内置）。

## 最近完成
- D4：Gateway（`/gw/*path`、路由表 CRUD、round_robin/hash 策略、健康摘除、2 候选重试、503/501 语义、免 token）+ `InstanceSpec.trigger` 贯通心跳 + agent spec 变更自动替换 manager。

## 下一步（D5a）
- PlaneRelayBus：`sink.queue` 发布 → agent 上报 plane → 按订阅表转发给订阅实例所在 agent → 注入本机 InProcessBus → queue 触发器消费；有界积压 + 超限丢弃告警；Gateway `mode: bus` 接上真实分发（替换 501）。
- 依据：`docs/design/distributed-plane-and-dashboard.md` §8 D5a（v2 决策：内置中转起步）。

## 参考
- 分布式设计：`docs/design/distributed-plane-and-dashboard.md`（v2，D1–D4 已标 ✅）
- 质量门禁：`scripts/check-file-length.sh`、`scripts/check-fn-length.py`
