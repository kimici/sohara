# 经验与调研报告：tokio `write_all` 缓冲写导致的「写后即读为空」竞态

> 日期：2026-08-16 ｜ 范围：Sohara 项目 CI 测试失败排查 ｜ 结论：已修复并验证
> 相关提交：`5005153`（修复）、`.agents/worklog/2026-08-16.md`（过程记录）

## 1. 摘要

GitHub Actions（ubuntu-latest）上，多个「执行流程写文件、测试立即读回并断言内容」的用例随机失败——断言消息显示**文件内容为空**（`got: ` 后面什么都没有）。现象有三个显著特征：

- **平台相关**：macOS 本地从未复现，Linux（CI 与本地 Linux VM）稳定复现；
- **与业务无关**：失败用例横跨 `sohara-js`（QuickJS 脚本）与 `sohara-builtins`（纯 filter/map），说明不是 QuickJS 的问题；
- **事后文件总是正确的**：失败之后查看磁盘，文件内容完好。

根因最终定位在 **tokio `File::poll_write` 的内部缓冲语义**：`write_all` 在最后一个数据块被「接收入内部缓冲」时即返回 `Ok`，而真正的 `write(2)` 系统调用仍挂在阻塞池上尚未执行。修复是在 `write_all` 之后显式 `file.flush().await`，保证数据真实落盘后再报告成功。

## 2. 症状与首个错误方向

### 2.1 首次 CI 报告

第一个报错是 `sohara-js` 的 `script_transform_enriches_records`：

```
thread 'script_transform_enriches_records' (6062) panicked at sohara-js/tests/script.rs:40:5:
got: 
```

失败断言是 `assert!(output.contains("ALICE"), "got: {output}")`——文件读出来了，但是空字符串；同文件的 `stats.processed == 1` 断言通过，说明记录确实到达了 sink。

### 2.2 误入的排查方向（教训见 §7）

因为第一个报错来自 QuickJS 用例，且项目历史上有过 QuickJS 栈检测的坑（`patched` feature 修复），最初几轮把精力投在 QuickJS 上：

- 在 macOS 上用 1.91 / 1.97 两套工具链反复跑 js 测试：全绿；
- 在 workspace 内装了与 CI 同版本的 rust 1.97.1 再跑：全绿；
- 把 js 测试连跑 20 遍：全绿。

**决定性转折**是用户贴出的第二份 CI 日志：`sohara-builtins/tests/steps.rs` 里两个**完全不含 QuickJS** 的用例（`inline_filter_map_to_jsonl_file`、`csv_source_roundtrips_through_csv_sink`）以同样的「空文件」方式失败。这说明病灶在**所有 file sink 用户共享的路径**上，而不是脚本桥。

## 3. 复现环境的搭建

macOS 上无法复现，必须拿到 Linux 环境。本机可用的手段：

- Docker：镜像拉取失败（Keychain 凭据 + VM 内 CDN 网络不通）；
- `colima` Linux VM（Ubuntu 24.04 aarch64）：可 SSH 进入、可挂载宿主源码目录、有 gcc、apt 可装 `build-essential`。

最终方案：在 colima VM 内安装 rustup（1.97.1，与 CI 一致）、复用宿主 `.cargo-home` 的 crates 缓存、用独立 `CARGO_TARGET_DIR=/tmp/target-linux-vm` 编译，直接跑 `sohara-js` 与 `sohara-builtins` 的测试二进制。

**复现结果**：Linux VM 上 sohara-js 套件连跑 20 轮，18 轮失败（4–6 个用例中随机 1–2 个「空文件」）——问题变成了可稳定研究的对象。

## 4. 排查方法与关键证据

排查按「先观察、后假设」的顺序推进，每步用独立手段交叉验证：

### 4.1 应用层打点（排除顺序问题）

在 `FileSink::flush/write` 与测试读文件处加时间戳日志，得到看似矛盾的两类序列：

```
DBG-SINK write-open  (inode=530713)
DBG-SINK write-done            ← write_all 已返回 Ok
DBG-SINK flush-returned
DBG-TEST run-returned          ← run() 已返回
DBG-TEST stat: len=0 ino=530713  ← 同一 inode，长度却是 0！
```

**写入成功了、同一 inode、程序顺序上在读取之前——文件却是 0 字节。** 这排除了「读写在两个不同文件」的猜测，也排除了我们代码里 await 链断裂的可能性（链路逐行核对全部正确）。

### 4.2 系统调用级追踪（LD_PRELOAD）

手写 LD_PRELOAD 拦截器（colima VM 内有 gcc），拦截 `open/open64/openat/openat64` 与 `read/write`，按 fd 打路径标签、纳秒时间戳：

```
FS .285547673 open64 js-transform.jsonl  O_WRONLY|O_CREAT|O_TRUNC  fd13  tid=3516306  ← sink 打开(截断)
FS .285622134 open64 js-transform.jsonl  O_RDONLY                  fd14  tid=3516305  ← 测试打开读
FS .285624467 read  fd14 → 0 字节                                            tid=3516305  ← 读到 0！
FS .285829222 write fd13 → 17 字节                                           tid=3516306  ← 真正的写 280µs 后才发生！
```

**真相浮出水面**：sink 的 `write_all` 已返回成功，但 `write(2)` 系统调用在测试读完之后才执行。数据「迟到」了约 280µs——这正是 tokio 阻塞池的调度窗口。macOS 上这个窗口几乎总是写方获胜，Linux 上读方经常获胜，于是呈现平台差异。

### 4.3 源码确认（tokio）

阅读 workspace 内缓存的 tokio 源码 `fs/file.rs`，`AsyncWrite for File` 的 `poll_write`：

```rust
State::Idle(ref mut buf_cell) => {
    let mut buf = buf_cell.take().unwrap();
    ...
    let n = buf.copy_from(src, me.max_buf_size);      // 拷入内部缓冲
    let std = me.std.clone();
    let blocking_task_join_handle = spawn_mandatory_blocking(move || {
        ... buf.write_to(&mut &*std) ...             // 真正的写发生在阻塞池上
    })...;
    inner.state = State::Busy(blocking_task_join_handle);
    return Poll::Ready(Ok(n));                        // ← 立即返回 Ready！
}
```

而 `AsyncWriteExt::write_all` 的循环在收到 `Ready(Ok(n))` 后推进缓冲区，最后一个 chunk 被接受即返回 `Ok`——**从不等待 `State::Busy` 里的任务完成**。`FileSink::write` 返回 Ok → `flush` 返回 → `run()` 返回 → 测试读文件，读到的只是被 `O_TRUNC` 打开、尚未写入的空文件。

补充观察：文件句柄 `drop` 时数据仍在阻塞池上，所以「事后看文件总是对的」——写入最终完成，只是晚于测试的读取。

## 5. 修复

`sohara-builtins/src/file.rs` 的 `FileSink::write`，在 `write_all` 之后显式刷出：

```rust
file.write_all(content.as_bytes())
    .await
    .map_err(Error::Io)?;
// Tokio 的 write_all 在最后一个块被收入内部缓冲时即返回；真正的
// write(2) 可能仍在阻塞池上。必须 flush 让数据真实进入文件，
// 否则 flush 之后的立即读取会竞态（Linux 上实测必现）。
file.flush().await.map_err(Error::Io)?;
```

`poll_flush` 会等待挂起的 `State::Busy` 任务完成，因此这行代码把「写缓冲可见性」重新纳入 sink 成功返回的契约。

## 6. 验证

| 验证项 | 修复前 | 修复后 |
|---|---|---|
| Linux VM：sohara-js 套件 × 20 轮 | ~90% 失败（18/20） | **20/20 全绿** |
| Linux VM：sohara-builtins steps × 20 轮 | 必现失败 | **20/20 全绿** |
| macOS 全 workspace（rust 1.97.1） | 122 通过（未受影响） | 122 通过 |
| clippy `-D warnings` / fmt / 长度门禁 | — | 全过 |
| GitHub Actions（ubuntu-latest） | failure | **success** |

## 7. 经验与教训

1. **第一现场不等于根因**。首个报错的测试用了 QuickJS，但「空文件」这个症状属于 file sink——靠单一用例的栈信息会把人带偏。多问一句「哪些用例也这样失败」比深挖第一个用例更有效。
2. **`write_all` 的语义因实现而异**。`std::fs::File` 的 `write_all` 是同步系统调用循环，返回即落页缓存；tokio `File` 的 `write_all` 是「接受入缓冲」语义，最后一个块可能还在阻塞池上。异步 I/O 场景下，「写完」之后立刻「同进程读回」必须显式 `flush`（必要时 `sync_all`）。
3. **平台差异往往是调度差异**。macOS 从不复发、Linux 必现，这种「只在一个平台坏」的问题优先怀疑时序竞态而非业务逻辑；在目标平台（或近似平台）上复现是排查的前提。
4. **复现环境的性价比**。colima Linux VM + 复用宿主 cargo 缓存 + 独立 target 目录，让「CI-only」问题变成了本地可 20 连跑研究的对象；没有它这次排查几乎不可能收敛。
5. **内核级证据不可替代**。应用层日志会自相矛盾（写入成功、同一 inode、长度为 0），LD_PRELOAD 的系统调用轨迹直接给出「write 晚于 read 280µs」的铁证。排障工具箱里应常备一个轻量 syscall 追踪手段。
6. **看懂库的缓冲层再下结论**。tokio 的 File 缓冲写是刻意设计（减少小写开销），但它的「写完成」边界和我们直觉不一致；使用异步文件 API 时，把「数据可见」作为显式契约（flush）而不是隐含假设。

## 8. 参考

- tokio `fs/file.rs` 中 `AsyncWrite for File::poll_write` 与 `Inner::poll_flush` 的实现（本地缓存版本 1.x，仓库 `.cargo-home/registry/src/*/tokio-1.*/src/fs/file.rs`）。
- tokio 文档：`AsyncWriteExt::write_all` 不调用 `flush`；`File::flush`/`poll_flush` 会排空挂起的阻塞池写。
- 项目内记录：`.agents/worklog/2026-08-16.md`（排查全过程，含 LD_PRELOAD 工具与时间线数据）。
