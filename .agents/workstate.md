# Workstate

## 当前焦点
- sohara-js 延后功能已全部落地（S5/S6 宿主桥）：sohara.record/sleep/notify/file/http/db/require、ctx 能力（step/flow/state/log/fail/emit/checkpoint/env/var/correlation_id）、Record 方法（get/set/has/unset + id/timestamp/metadata/schema）、权限开关（allow: file.write/db/http/notify/all）；文档 quickjs-api.md 状态列已同步。

## 状态 / 阻塞
- 无阻塞。workspace 47 套测试全绿（sohara-js 22 例）、clippy -D warnings 0、fmt、长度门禁；待 commit + push。
- 沙箱环境约束：cargo 需 `CARGO_HOME=$HOME/workspace/fe/sohara/.cargo-home`。

## 最近完成
- sohara-js 模块重构（env/host/callbacks/bridge/step + assets/preamble.js）与 16 例新宿主测试；`ScriptConfig` 新增 `allow`/`db`；core `StepMeta`/`BuildContext{flow,step}`；graph 复用 `sohara-config::step_context`。
- 关键约束踩坑：quick-js 回调 `Arguments` 变参、`RefUnwindSafe` 包装（BusHandle）、reqwest blocking 需独立 OS 线程（tokio 上下文 drop panic）。

## 可选后续（路线图外 / 已标注延后）
- D5b NATS/JetStream + least-loaded；控制面 HA / mTLS / Gateway 前置 LB；实例 run-history 上传聚合。
- `sohara.registerStep`（quickjs-api §8，仍延后）；`ctx.state` checkpoint 持久化（本版进程内内存，重启清空）。

## 参考
- 排查报告：`docs/reports/tokio-write-visibility-race.md`（tokio 缓冲写竞态）
- 使用文档：`docs/usage.md`
- QuickJS API：`docs/design/quickjs-api.md`（已落地状态）
- 分布式设计：`docs/design/distributed-plane-and-dashboard.md`（v2，D1–D6 ✅）
- 质量门禁：`scripts/check-file-length.sh`、`scripts/check-fn-length.py`
