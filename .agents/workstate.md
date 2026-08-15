# Workstate

## 当前焦点
- 分布式管理层实施：**D1–D6 全部完成**（目标 goal-86bd2ea3 可收官）。D5b（NATS/JetStream）为可选后期项。

## 状态 / 阻塞
- 无阻塞。121 测试全绿、clippy 0、fmt、长度门禁；D1–D6 各阶段均有测试 + 真实进程 e2e 证据。
- 沙箱环境约束：cargo 需 `CARGO_HOME=/Users/qliu23/workspace/fe/sohara/.cargo-home`；本机 HTTP_PROXY=127.0.0.1:7890（本地 HTTP 已 no_proxy）。

## 最近完成
- D6：Manager UI（/ui：实例矩阵/生命周期/声明/路由/事件历史）、状态直查代理（admin token 透传）、集群事件环、三向 token 收尾（plane/实例 admin/relay）。

## 可选后续（路线图外）
- D5b NATS/JetStream + least-loaded 策略。
- Gateway 前置 LB / 控制面 HA、mTLS。
- 各实例 run history 上传聚合（当前 plane 侧为事件历史，单机完整报告在 /admin/ui）。

## 参考
- 分布式设计：`docs/design/distributed-plane-and-dashboard.md`（v2，D1–D6 已标 ✅）
- 质量门禁：`scripts/check-file-length.sh`、`scripts/check-fn-length.py`
