# Workstate

## 当前焦点
- 分布式管理层实施中（目标 goal-86bd2ea3）：**D1 ✅、D2 ✅、D3 ✅、D4 ✅、D5a ✅**，下一步 **D6 全局 Dashboard + 安全收尾**（D5b NATS 为可选后期项）。

## 状态 / 阻塞
- 无阻塞。118 测试全绿、clippy 0、fmt、长度门禁；跨机事件总线 e2e（实例间 + Gateway bus）验证通过。
- 沙箱环境约束：cargo 需 `CARGO_HOME=/Users/qliu23/workspace/fe/sohara/.cargo-home`；本机 HTTP_PROXY=127.0.0.1:7890，本地 HTTP 需 no_proxy（已内置）。

## 最近完成
- D5a：RelayBus 桥接（triggers）+ serve `--relay` + plane 中继邮箱（/relay/publish|pull、有界积压、游标）+ Gateway bus 真实分发 + agent spec 透传；e2e 两条路径消息到达。

## 下一步（D6）
- 全局 Dashboard（Manager UI）：总览（节点/实例健康矩阵）、实例详情（聚合 + 直查）、调度配置、部署、跨实例运行历史（agent 上报单机 history）。
- 安全收尾：三向 token 全链路（plane/agent/实例 relay/admin）、可选 mTLS、Gateway 前置 LB 评估。
- 依据：`docs/design/distributed-plane-and-dashboard.md` §8 D6。

## 参考
- 分布式设计：`docs/design/distributed-plane-and-dashboard.md`（v2，D1–D5a 已标 ✅）
- 质量门禁：`scripts/check-file-length.sh`、`scripts/check-fn-length.py`
