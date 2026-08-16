# Workstate

## 当前焦点
- 分布式管理层（D1–D6）与使用文档已完成；vnv 两项修复（tags 文档一致性、中继重放语义 + 稳定订阅者游标下限）已落地。

## 状态 / 阻塞
- 无阻塞。122 测试全绿、clippy 0、fmt、长度门禁；工作区待提交本轮修复与文档。
- 沙箱环境约束：cargo 需 `CARGO_HOME=$HOME/workspace/fe/sohara/.cargo-home`。

## 最近完成
- vnv 修复：`tags` 标注一致化；RelayBus 稳定订阅者 id（admin 地址）+ plane 游标下限（实例重启不重放，plane 重启重放邮箱尾部——已文档化）。
- 新增 `docs/usage.md`（单机/分布式全量使用手册 + 排查 FAQ）。

## 可选后续（路线图外）
- D5b NATS/JetStream + least-loaded；控制面 HA / mTLS / Gateway 前置 LB；实例 run-history 上传聚合。

## 参考
- 使用文档：`docs/usage.md`
- 分布式设计：`docs/design/distributed-plane-and-dashboard.md`（v2，D1–D6 ✅）
- 质量门禁：`scripts/check-file-length.sh`、`scripts/check-fn-length.py`
