# Workstate

## 当前焦点
- 已完成 **A1：`ctx.state` / `ctx.checkpoint()` 接到 runtime `StateStore`**，以及 **A2：`sohara.registerStep()` 的 per-flow 两遍构建**。
- 已完成 **D1 的首批落地**：QuickJS `sohara.flow(...)` builder + `.save()` + CLI 直接加载 `.js` flow 文件（`run/serve/approve`）。
- 已完成 **D2 的首批落地**：`sohara.flow.run(config)` + `{ stop() }` handle，支持脚本内直接运行子 flow。
- 扩展主线仍是 **subprocess IPC / JSON-RPC over stdio**，当前主机侧已覆盖 `source` / `transform` / `sink` / `trigger` / `state-store` / `event-bus`，并开始内置 **Rust builtin** 扩展集。

## 状态 / 阻塞
- 无阻塞。
- 当前实现范围明确：generic `EventBus` 仍是 **publish-only**，不能替代 built-in `queue` trigger 的 subscribe 路径；provider-specific 的 subscribe 则通过 paired trigger（如 `builtin-sqlite-trigger`、`builtin-zeromq-trigger`）来补齐。
- 当前测试覆盖：sqlite 与 zeromq 的 provider-specific bus ↔ trigger 都已进默认集成测试；redis 仍因外部服务依赖未进同等级默认端到端覆盖。

## 最近完成
- `docs/design/next-roadmap.md` 改为 draft v9：状态对齐到当前实现，明确 A1/A2、Phase B v1、D1、D2 首批已落地。
- `docs/usage.md`：命令入口不再写死 YAML，补充 `.js` flow 直跑说明。
- `examples/README.md` + `examples/js-flow.js`：新增 QuickJS flow builder 示例索引与可运行示例。
- `docs/design/next-roadmap.md` 改为 draft v6：Phase B 扩到 trigger/state-store/event-bus，并显式记录 publish-only external event_bus 的限制。
- `docs/design/extension-points.md` 对齐：声明当前活跃 scope 已覆盖 source/transform/sink/trigger/state-store/event-bus。
- `docs/design/next-roadmap.md` 改为 draft v7：增加 builtin-* 命名空间策略与 builtin sqlite/redis/zeromq 扩展集。
- `docs/design/next-roadmap.md` 改为 draft v8：builtin 语义纠正为 Rust executables，而不是脚本。
- `sohara-config/src/lib.rs` + `paths.rs`：新增顶层 `event_bus` 配置，以及 `checkpoint.store` 的 string-or-object 形式。
- `sohara-runtime/src/stdio_extensions.rs`：新增 external trigger builder、blocking state-store / event-bus client、相应 JSON-RPC 方法。
- `sohara-runtime/src/serve.rs`：接入 external trigger host 与 shared bus override。
- `sohara-cli/src/main.rs`：`run` / `serve` / `approve` 新增 `--extensions PATH`，并自动加载 trusted builtin manifests。
- `sohara-builtin-extensions/`：新增 Rust builtin crate（sqlite / redis / zeromq binaries + shared helper）。
- `extensions/builtin/`：builtin manifests 已切到 Rust binaries；原 Python builtin 脚本已删除。
- `sohara-builtin-extensions/src/bin/sohara-builtin-sqlite.rs`：补上 `builtin-sqlite-trigger`，通过轮询同一 SQLite `event_bus` 表完成 subscribe。
- `sohara-builtin-extensions/src/bin/sohara-builtin-zeromq.rs`：从 placeholder 改为真实 Rust zeromq bus/trigger binary。
- `sohara-runtime/tests/stdio_extensions.rs`：覆盖 external source / transform / sink / trigger / state-store / event-bus 集成场景。
- `sohara-runtime/tests/stdio_extensions.rs`：补充 builtin namespace 保护与 trusted builtin sqlite store 覆盖。
- `sohara-runtime/tests/stdio_extensions.rs`：新增 trusted builtin sqlite bus ↔ trigger 配对覆盖。
- `sohara-runtime/tests/stdio_extensions.rs`：新增 trusted builtin zeromq bus ↔ trigger 默认覆盖，当前采用 trigger bind / bus connect 拓扑以规避 crate 的 connect-before-bind 阻塞。
- `sohara-core/src/store.rs` + `registry.rs`：新增 `StateAccessor` 与 `BuildContext.state_accessor`。
- `sohara-runtime/src/persist.rs` + `executor.rs`：runtime 状态改为单一权威共享体；脚本状态恢复可经 `--resume` 读取 store。
- `sohara-js/src/{env,bridge,host,callbacks,lib}.rs`：`ctx.state` 读写与 `ctx.checkpoint()` 已接到 runtime state bridge。
- `sohara-runtime/tests/persistence.rs`：新增 script ctx.state 经 checkpoint + resume 恢复的集成覆盖。
- `docs/design/quickjs-api.md`：同步 `ctx.state` / `ctx.checkpoint()` 的真实语义。
- `sohara-core/src/registry.rs` + `sohara-config/src/build.rs`：新增 `RegisteredStep` 与 flow 级两遍构建。
- `sohara-js/assets/preamble.js` + `callbacks.rs` + `host.rs`：新增 `sohara.registerStep()` / `__register_step` 注册链路。
- `sohara-runtime/tests/script_registration.rs`：新增脚本注册步骤的端到端覆盖。
- `sohara-js/src/flow.rs` + `assets/preamble.js`：新增 `sohara.flow(...)` builder、`save()` 与 JS flow 文件求值。
- `sohara-config/src/lib.rs`：新增 `FlowConfig::from_json_value(...)`。
- `sohara-cli/src/main.rs`：`run` / `serve` / `approve` 现在接受 `.js` flow 文件。
- `sohara-js/tests/flow_api.rs`：新增 JS flow build/save/run 端到端覆盖。
- `sohara-core/src/flow_runner.rs`：新增脚本子 flow runner 抽象。
- `sohara-runtime/src/subflow.rs`：新增 `RuntimeFlowRunner`，负责有限/带 trigger 子 flow 的运行与 stop。
- `sohara-js/src/{env,host,callbacks,bridge}.rs`：接入 `sohara.flow.run(...)` / `handle.stop()` 与 invocation-scope cleanup。
- `sohara-runtime/tests/flow_run_api.rs`：新增有限子 flow 与 trigger 子 flow stop handle 覆盖。
- `sohara-js/tests/host_core.rs`：补充 `sohara.sleep(整数)` 与 `sohara.flow.run()` 最小桥接测试。

## 下一步
- 给 stdio 扩展补充请求超时 / 子进程重启 / 更明确的错误模型。
- 对 A0a/A3 async bridge 给出最终结论（落地 or 明确冻结同步基线）。
- 继续打磨 D2：相对路径基准、更明确的 sub-flow timeout / restart policy、以及文档细化。
- 设计总线订阅抽象，解决 external event_bus 与 built-in queue trigger 的语义缺口。
- 提供真实示例扩展目录（Python/Node 至少一种），而不仅是测试夹具。
- 为 builtin redis 增加同等级端到端环境验证与文档。
- 再评估是否需要 `SOHARA_EXTENSIONS` 环境变量、目录递归扫描、能力协商升级。

## 参考
- **路线图**：`docs/design/next-roadmap.md`
- **扩展点**：`docs/design/extension-points.md`
- **实现入口**：`sohara-runtime/src/stdio_extensions.rs`
- **CLI 入口**：`sohara-cli/src/main.rs`
- **运行时接线**：`sohara-runtime/src/serve.rs`
- **配置模型**：`sohara-config/src/lib.rs`
- **测试**：`sohara-runtime/tests/stdio_extensions.rs`
