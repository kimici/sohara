# Sohara QuickJS API

> 状态：已落地（S5/S6 实现，2026-08-17 更新）
> 本文定义 `script` 步骤与 QuickJS 宿主桥接的完整 API 契约，是 `redesign-and-roadmap.md` §2.1/§2.6/§2.8 的细化。
> 实现位置：`sohara-js/src/{env,host,callbacks,bridge,step}.rs` + `sohara-js/assets/preamble.js`。

---

## 目录

1. [运行模型](#1-运行模型)
2. [脚本步骤与入口约定](#2-脚本步骤与入口约定)
3. [全局对象 `sohara`](#3-全局对象-sohara)
4. [Record API](#4-record-api)
5. [ExecutionContext（`ctx`）](#5-executioncontextctx)
6. [模块加载 `require`](#6-模块加载-require)
7. [异步桥接](#7-异步桥接)
8. [步骤注册 `sohara.registerStep`](#8-步骤注册-sohararegisterstep)
9. [示例](#9-示例)
10. [安全与限制](#10-安全与限制)

---

## 1. 运行模型

- 引擎：**QuickJS**（`quick-js` crate），每个 `script` 步骤一个隔离的 JS 上下文（context），步骤间不共享可变全局状态。
- 每个脚本上下文预注入一个全局对象 **`sohara`**（宿主桥），并支持 `require("sohara")` 返回同一对象。
- 脚本按约定提供**入口函数**；运行时按 `kind` 调用对应签名（见 §2）。
- 默认同步语义（学 rec 的 Rhino 全同步）：宿主函数阻塞执行；异步桥接（Promise/`await`）为**可选增强**，需先经 spike 验证 `quick-js` 驱动 Promise 的可行性（见 §7）。

---

## 2. 脚本步骤与入口约定

`script` 步骤在 YAML 中的声明：

```yaml
# 文件形式
- { id: enrich, kind: transform, type: script, config: { script: enrich.js, entry: transform } }
# 内联形式
- { id: enrich, kind: transform, type: script, config: { inline: "function transform(r, ctx){ ... }" } }
```

| 字段 | 说明 |
|---|---|
| `script` | 脚本文件路径（相对 flow 文件目录；`require` 相对其目录解析） |
| `inline` | 内联脚本字符串（与 `script` 二选一） |
| `entry` | 入口函数名，缺省按 kind：`generate`(source) / `transform`(transform) / `consume`(sink) |
| `allow` | 权限列表（`file.write` / `db` / `http` / `notify` / `all`，见 §10） |
| `db` | SQLite 数据库路径（`sohara.db.query` 使用，需 `allow: [db]`） |
| `timeout` | 脚本执行超时（预留） |

按 `kind` 约定的入口签名：

| kind | 入口 | 签名 | 返回值语义 |
|---|---|---|---|
| source | `generate(ctx)` | `(ctx) => record\|record[]\|void` | 返回/`ctx.emit` 的记录作为源输出 |
| transform | `transform(record, ctx)` | `(record, ctx) => record\|null\|record[]` | `record` 继续；`null` 过滤；数组扇出 |
| sink | `consume(record, ctx)` | `(record, ctx) => void` | 无返回值 |

```js
// transform 示例：返回 null 即过滤
function transform(record, ctx) {
  if (record.age < 18) return null;
  record.adult = true;
  return record;
}
```

脚本也可以顶层执行（side-effect，仅做初始化），此时运行时跳过入口调用；入口函数在脚本顶层定义即可，不强制 `export`（QuickJS 无原生 ESM，见 §6）。

---

## 3. 全局对象 `sohara`

宿主桥，提供日志、状态、环境、I/O、事件、构造等能力。

| 成员 | 签名 | 状态 | 说明 |
|---|---|---|---|
| `sohara.log(level, msg)` | `(string, string)` | 已实现 | 日志；level ∈ `debug/info/warn/error` |
| `sohara.fail(msg)` | `(string?)` | 已实现 | 抛出步骤失败 |
| `sohara.json` | `{ parse, stringify }` | 已实现 | JSON 工具（转发 `JSON`） |
| `sohara.env(name, fallback?)` | `(string, any?) => any\|undefined` | 已实现 | 读环境变量；未定义时返回 fallback |
| `sohara.var(name, fallback?)` | `(string, any?) => any\|undefined` | 已实现 | 读流程变量；未定义时返回 fallback；值为原生 JSON（数字保持 number） |
| `sohara.now()` | `() => string` | 已实现 | 当前时间（ISO8601） |
| `sohara.uuid()` | `() => string` | 已实现 | UUID v4 |
| `sohara.record(data)` | `(object) => Record` | 已实现 | 构造 Record（同 §4 方法） |
| `sohara.sleep(ms)` | `(number) => void` | 已实现 | 阻塞延时，`ms` clamp 到 `[0, 60000]` |
| `sohara.notify(topic, payload)` | `(string, any) => void` | 已实现 | 投递事件总线（serve 模式；需 `allow: [notify]`） |
| `sohara.file.read(path)` / `sohara.file.write(path, content)` | — | 已实现 | 文件 I/O；`read` 恒允许，`write` 需 `allow: [file.write]` |
| `sohara.http.request(opts)` | `(opts) => Response` | 已实现 | 阻塞 HTTP 请求（需 `allow: [http]`；在独立 OS 线程执行，避免 tokio 上下文冲突） |
| `sohara.db.query(sql, params?)` | `(string, array?) => object[]` | 已实现 | SQLite 查询（需 `allow: [db]` + `db` 路径；阻塞） |
| `sohara.registerStep(kind, type, handler)` | `(string, string, function)` | 延后 | 注册自定义步骤（见 §8） |
| `sohara.require(path)` | `(string) => any` | 已实现 | 加载相对模块（同全局 `require`，见 §6） |

```js
sohara.log("info", "processing", record.id);
const key = sohara.env("API_KEY", "dev-key");
sohara.notify("logs", { id: record.id });
const res = sohara.http.request({ url: "https://api/geo", method: "GET" });
```

`sohara.http.request` 的 `opts`：`{ url, method?, headers?, body?, timeout_ms? }`（默认 `GET`、超时 10s，clamp 到 `[1s, 120s]`）；返回 `{ status, ok, headers, text, json }`。

---

## 4. Record API

记录在脚本里是**普通 JSON 对象**（已实现语义）：字段直接读写，`transform` 返回的对象成为新 payload；原地修改同样生效。

```js
function transform(record, ctx) {
  record.full_name = record.first + " " + record.last;
  record.tags = ["new", "vip"];
  return record;   // 或返回新对象；null/undefined 过滤；数组扇出
}
```

**方法版 Record API（已实现）**：每个入口收到的 `record` 都是带方法的 `Record`（`Object.create` 原型，字段即 payload 属性，非枚举成员不进入 `JSON.stringify`）：

| 成员 | 说明 |
|---|---|
| `record.get(path)` | 读取 `path`（点分路径，如 `"a.b.c"`；无点号即直接字段） |
| `record.set(path, value)` | 按点分路径写入（自动创建中间对象） |
| `record.has(path)` | `get(path) !== undefined` |
| `record.unset(path)` | 按点分路径删除（中间路径不存在则 no-op） |
| `record.toJSON()` | 仅返回 payload 字段（不含 `id/timestamp/__meta` 等非枚举成员） |
| `record.id` / `record.timestamp` | 只读；当前记录的 id / ISO8601 时间戳（来自运行时元信息） |
| `record.metadata` | 只读；记录的元信息对象（可能为 `undefined`） |
| `record.schema` | 只读；恒为 `null`（schema 为 S5 可选增强，未实现） |

> `sohara.record(data)` 返回同构对象（无运行时元信息，`id/timestamp` 为 `undefined`）。

---

## 5. ExecutionContext（`ctx`）

**已实现**：入口函数签名统一为 `(record, ctx)`（source 为 `(ctx)`）；`ctx` 由桥接层构造（`__makeCtx`），同时保留全局 `__ctx` 兼容旧脚本（恒为无步骤信息的空 ctx）。

| 成员 | 类型 | 状态 | 说明 |
|---|---|---|---|
| `ctx.step` | `{ id, name, kind, type }` | 已实现 | 当前步骤元信息（来自构建期 `StepMeta`） |
| `ctx.flow` | `{ name, version }` | 已实现 | 流程元信息（`version` 恒为 `"1"`） |
| `ctx.state` | object | 已实现 | 步骤累加状态，可读写；**本版为进程内 per-step 内存**（跨记录共享；`__state_sync` 在每次入口返回后回写） |
| `ctx.log(level?, ...msg)` | function | 已实现 | 等价 `sohara.log` |
| `ctx.fail(msg?)` | function | 已实现 | 抛出步骤失败（进入错误策略） |
| `ctx.emit(record)` | function | 已实现 | 产出额外记录：transform 的 `Pass` + 发射 → `Expand(pass + emitted)`；`Filtered` + 发射 → `Expand(emitted)`；`Expand` 追加 |
| `ctx.checkpoint()` | function | 已实现（no-op） | 请求一次 checkpoint；本版仅 debug 日志，持久化延后 |
| `ctx.env(name, fallback?)` | function | 已实现 | 读环境变量（等价 `sohara.env`） |
| `ctx.var(name, fallback?)` | function | 已实现 | 读流程变量 |
| `ctx.correlation_id` | string | 已实现 | 本次调用关联 id（每次调用生成 UUID v4） |

**状态持久化约定**：`ctx.state` 只接受 JSON 可序列化对象；脚本返回后运行时统一回写。**checkpoint 持久化（重启恢复）为延后项**——当前 `ctx.state` 仅存在于进程内存，serve 重启即清空。

---

## 6. 模块加载 `require`

**已实现**：QuickJS 无原生 ESM，Sohara 提供轻量 CommonJS 风格加载器（per-context `Map` 缓存；`require("sohara")` 返回宿主桥）：

```js
// lib.js
module.exports = { normalize: (s) => s.trim().toLowerCase() };
```

```js
// 主脚本
const lib = require("./lib.js");
function transform(record, ctx) {
  record.set("email", lib.normalize(record.get("email")));
  return record;
}
```

**解析顺序**：

1. `require("sohara")` → 返回全局宿主桥。
2. `require("./x")` / `require("../x")` → 相对当前脚本目录，支持 `.js`。
3. `require("pkg-name")` → 解析宿主注册的内置 JS 模块（当前仅内置 `sohara`；注意 `registerStep` 注册的是**步骤**而非 require 模块，见 §8）。

**能力边界**：MVP 支持「文件模块 + `module.exports` + `require` 缓存」；不支持 Node 内置模块（`fs/http` 等一律经 `sohara.*` 宿主 API 访问）。

---

## 7. 异步桥接（可选增强）

> 地基决策（定稿）：MVP 采用**同步宿主调用**（对齐 rec 的 Rhino 全同步），把异步复杂度挡在 S5 之外。异步桥接是可选增强，**不承诺**支持 `await`；实现前必须先做 spike 验证 `quick-js` 能否可靠驱动 Promise/continuation。

- **基线（同步，已落地）**：全部宿主 API 均为**阻塞**调用——`log/env/var/now/uuid/record/sleep/file/http/db/notify/require/emit/state_sync` 直接在当前执行线程完成并返回；脚本内**不要写 `await`**（`sohara.http.request` 等返回普通对象而非 Promise）。
- `sohara.sleep(ms)` 为阻塞 `thread::sleep`（`ms` clamp 到 `[0, 60000]`）；`sohara.http.request` 在独立 OS 线程执行（`reqwest::blocking` 在其 wait 逻辑里会探测 tokio 运行时，直接在运行时线程上调用会 panic），调用线程 join 等待。
- 启用异步桥接后：以上 API 改为返回 Promise，由运行时「挂起 QuickJS + tokio 回调」驱动，脚本可用 `await`/`.then()`。
- 脚本执行超时受 `timeout` 约束；超时视为步骤失败，进入 `on_error` 策略（`retry` 为其一种）。

```js
function transform(record, ctx) {
  const res = sohara.http.request({ url: "https://api/geo", method: "GET" });
  record.set("geo", res.json);
  return record;
}
```

---

## 8. 步骤注册 `sohara.registerStep`

> 状态：**延后，未实现**（保持设计）。当前自定义步骤一律走 Rust 库注册（`ComponentRegistry`）。

允许脚本内注册自定义 `(kind, type)` 步骤，等价于向 `ComponentRegistry` 动态扩展（见 `redesign-and-roadmap.md` §2.6）：

```js
sohara.registerStep("transform", "slugify", function(record, ctx) {
  record.set("slug", String(record.get("name")).toLowerCase().replace(/\s+/g, "-"));
  return record;
});
```

- 注册作用域：**仅当前脚本上下文**（步骤内隔离，不共享可变全局）；跨流程/全局扩展一律走 Rust 库（编译期注册），保持隔离与安全。
- handler 签名与 §2 入口一致，按 `kind` 调用。
- 注册后 YAML 中即可 `type: slugify`。

---

## 9. 示例

### 9.1 source 脚本（生成记录）

```js
function generate(ctx) {
  for (let i = 0; i < 10; i++) {
    ctx.emit(sohara.record({ n: i }));
  }
}
```

### 9.2 transform 脚本（富化 + 过滤 + 扇出）

```js
function transform(record, ctx) {
  if (record.get("amount") <= 0) return null;          // 过滤
  record.set("level", record.get("amount") > 1000 ? "high" : "normal");
  const rows = record.get("items") || [];
  return rows.map(item => sohara.record({ ...record.toJSON(), item })); // 扇出
}
```

### 9.3 sink 脚本（副作用）

```js
function consume(record, ctx) {
  sohara.log("info", "sink:", record.id);
  ctx.state.count = (ctx.state.count || 0) + 1;
}
```

### 9.4 结合 YAML

```yaml
steps:
  - { id: enrich, kind: transform, type: script, config: { script: enrich.js } }
  - { id: out, kind: sink, type: log }
edges: [[enrich, out]]
```

---

## 10. 安全与限制

- **沙箱**：脚本默认只读访问 `sohara.file.read`；`sohara.file.write`、`sohara.db.query`、`sohara.http.request`、`sohara.notify` 按步骤 `allow` 权限开关启用（`file.write` / `db` / `http` / `notify`，或 `all` 全开；未授权调用抛 JS 错误进入 `on_error` 策略）。
- **db 路径**：`sohara.db.query` 使用步骤配置的 `db` 路径（每次查询重开 SQLite 连接，无跨步骤共享连接）。
- **资源上限**：单脚本 `timeout`（默认如 30s）、内存上限、`loop/foreach` 的 `max_iterations` 保护、禁止无限递归（由 QuickJS 栈/时间限制）。
- **确定性**：脚本应尽量无副作用；跨记录共享状态一律经 `ctx.state`（本版进程内内存，重启清空），不依赖脚本内模块级可变全局（上下文按步骤隔离，重启后重建）。
- **类型安全**：`record.set` 接受 JSON 兼容值；强类型 `schema` 下做校验（未实现）；脚本抛错 → 步骤失败 → 走 `on_error` 策略。
