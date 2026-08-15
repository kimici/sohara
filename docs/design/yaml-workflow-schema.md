# Sohara YAML Workflow Schema

> 状态：草案（draft），随 S1–S5 各阶段逐步落地
> 本文是 `redesign-and-roadmap.md` §2.7 的权威细化，定义 Flow 的声明式描述格式。schema 版本 `"1"`。

---

## 目录

1. [顶层文档](#1-顶层文档)
2. [Step 通用字段](#2-step-通用字段)
3. [kind 与 type 一览](#3-kind-与-type-一览)
4. [各 type 的 config 明细](#4-各-type-的-config-明细)
5. [edges 语法](#5-edges-语法)
6. [表达式语言](#6-表达式语言)
7. [imports 与 templates](#7-imports-与-templates)
8. [triggers（serve 模式）](#8-triggersserve-模式)
9. [状态 / 重试 / 检查点（S4）](#9-状态--重试--检查点s4)
10. [校验规则](#10-校验规则)
11. [完整示例](#11-完整示例)
12. [schema 版本演进](#12-schema-版本演进)

---

## 1. 顶层文档

一个 Flow 文件对应一次 `sohara run` 或 `sohara serve`。

```yaml
name: example-flow            # 必填，kebab-case，全局唯一命名
description: ...              # 可选
version: "1"                  # 必填，schema 兼容版本
imports:                      # 可选，YAML 片段/模板复用
  - common.yaml
vars:                         # 可选，流程变量默认值（可用 ${expr} 引用 env）
  api_base: "${env:API_BASE}"
env:                          # 可选，声明运行时需要的环境变量（校验用）
  - DATABASE_URL
config:                       # 可选，流程级默认配置（type 级 defaults 合并规则随 S2 细化）
  defaults:
    file: { encoding: utf-8 }
templates:                    # 可选，可复用步骤模板（见 §7）
  normalize: { kind: transform, type: map, config: { ... } }
triggers: []                  # 可选，serve 模式的入口（见 §8）
steps: []                     # 必填，步骤列表
edges: []                     # 可选，缺省时按 steps 声明顺序线性串联（受 §5 约束）
checkpoint: {}                # 可选，S4（见 §9）
```

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `name` | string | ✅ | `[a-z0-9][a-z0-9-]*` |
| `version` | string | ✅ | 当前 `"1"` |
| `description` | string | — | 描述 |
| `imports` | string[] | — | 相对路径/glob，导入模板与命名步骤 |
| `vars` | map | — | 流程变量默认值 |
| `env` | string[] | — | 声明的环境变量名 |
| `config` | map | — | 流程级默认配置 |
| `templates` | map<string, Step> | — | 可复用步骤模板 |
| `triggers` | Trigger[] | — | serve 模式入口 |
| `steps` | Step[] | ✅ | 至少 1 个 |
| `edges` | Edge[] | — | 显式边 |
| `checkpoint` | Checkpoint | — | 检查点策略 |

---

## 2. Step 通用字段

```yaml
- id: step-1                 # 必填，流程内唯一，[a-zA-Z_][a-zA-Z0-9_.-]*
  kind: transform            # 必填：source | transform | sink | control
  type: filter               # 必填：组件名（同 kind 命名空间内唯一）
  name: "过滤成年用户"         # 可选，展示名
  description: ...           # 可选
  config: { ... }            # 组件配置（推荐）；也可用扁平简写（见下）
  use: normalize             # 可选：引用 templates 中的模板（与 config 合并）
  when: "status == 'active'" # 可选：步骤级前置条件，不满足则跳过
  timeout: 30s               # 可选：步骤超时
  on_error: fail             # 可选（S4）：fail | continue | retry（默认 fail）
  retry: { max: 3, backoff: 1s, on: [timeout, io] }  # 可选（S4）：仅 on_error: retry 时生效
  state: { ... }             # 可选（S4）：步骤累加状态初始值
  inputs: [a, b]             # 可选：显式入边（与 edges 二选一语义等价）
```

**配置写法**：推荐 `config:` 嵌套；同时支持「扁平简写」——把 `config` 里的字段直接写在 step 层级（`id/kind/type/name/description/config/use/when/timeout/on_error/retry/state/inputs` 为保留字除外）。二者等价：

```yaml
# 等价写法 A
- { id: out, kind: sink, type: file, config: { format: jsonl, path: out.jsonl } }
# 等价写法 B（扁平简写）
- { id: out, kind: sink, type: file, format: jsonl, path: out.jsonl }
```

**字段校验（定稿）**：默认 strict——扁平简写的合法键 = 该 `type` 的 config 字段 + 保留字；其余未知键（含拼写错误如 `formt`）**报错**，避免被静默吞掉。

**when 语义**：对每条进入的记录求值；为假则丢弃（不进入该步骤，也不传给下游）。

---

## 3. kind 与 type 一览

| kind | type | 引入阶段 | 作用 |
|---|---|---|---|
| source | `file` | S1 | 读文件（csv/json/jsonl） |
| source | `inline` | S0/S1 | 内置测试/示例记录（对应 VecSource） |
| source | `db` | S5 | SQL 查询结果 |
| source | `http` | S5 | HTTP 拉取（run 模式轮询/一次性） |
| source | `queue` | S3 | 订阅事件总线主题 |
| source | `manual` | S3 | 手工触发（CLI/管理 API 注入） |
| source | `script` | S5 | QuickJS 生成记录 |
| transform | `map` | S1 | 字段变换/投影 |
| transform | `filter` | S1 | 条件过滤 |
| transform | `add_field` | S1 | 增加/覆盖字段 |
| transform | `assert` | S0/S1 | 断言/校验（expect） |
| transform | `aggregate` | S2 | 分组聚合 |
| transform | `merge` | S2 | 合并上游多条/与另一源合并 |
| transform | `join` | 延后 | 关联连接（未实现） |
| transform | `split` | S2 | 一对多扇出 |
| transform | `batch` | S2 | 按 N 条/时间窗口成批 |
| transform | `dedup` | S2 | 去重 |
| transform | `script` | S5 | QuickJS 任意变换 |
| sink | `file` | S1 | 写文件 |
| sink | `db` | S5 | 写数据库 |
| sink | `http` | S5 | HTTP 投递 |
| sink | `queue` | S3 | 投递事件总线 |
| sink | `log` | S1 | 日志输出 |
| sink | `noop` | S1 | 丢弃（dummy） |
| sink | `collect` | S1 | 收集到内存（测试用） |
| sink | `email` | 延后 | 邮件输出（未实现） |
| control | `switch` | S2 | 多分支路由 |
| control | `foreach` | S2 | 对数组每项循环 |
| control | `loop` | S2 | 条件循环 |
| control | `parallel` | S2 | 并发扇出 |
| control | `join` | S2 | 汇聚（all/any/n） |
| control | `delay` | S3 | 延时 |
| control | `approve` | S4 | 人工审批（human-in-the-loop） |
| control | `subflow` | 延后 | 内联/引用子流程（未实现） |

> 类型命名即「组件注册表」的 `(kind, type)` 键（见 `redesign-and-roadmap.md` §2.6）。
> 分布式（多节点）、`db-watch`/`push` 触发器、AI agent 均**不在本路线图**，作为未来扩展点预留。

---

## 4. 各 type 的 config 明细

### 4.1 source

**`file`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `path` | string | 文件路径（支持 glob 时见 `expand`） |
| `format` | enum | `csv` \| `json` \| `jsonl`（`parquet`/`text` 延后） |
| `columns` | string[] | 列名（csv/text 无表头时必填） |
| `delimiter` | string | 分隔符，默认 `,` |
| `header` | bool | 是否有表头，默认 csv 为 true |
| `schema` | Schema | 可选类型化 schema（S5，见 §4.4.1） |
| `encoding` | string | 默认 `utf-8` |

**`inline`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `records` | array | 记录数组，每项为 object |

**`db`**（S5，已实现）

| 字段 | 类型 | 说明 |
|---|---|---|
| `path` | string | SQLite 文件路径（相对流程文件目录） |
| `query` | string | SQL 查询；每行一条记录 |

> `url`/`params`/`batch_size`（连接串与分批拉取）未实现，延后。

**`http`**（S5，已实现）

| 字段 | 类型 | 说明 |
|---|---|---|
| `url` | string | 请求地址（仅 `http://`，S5） |
| `method` | string | 默认 GET |
| `headers` | map | 请求头 |
| `poll_interval` | duration | 轮询间隔（可选，设置后持续轮询） |

> `body` 字段未实现，延后。

**`queue`**（S3）

| 字段 | 类型 | 说明 |
|---|---|---|
| `topic` | string | 订阅主题 |

**`manual`**（S3）：`{}`——由 CLI/管理 API 手工注入一条 `Record`。

**`script`**（S5，已实现）：见 `quickjs-api.md`；`script` 字段为文件或 `inline`，可选 `entry` 指定入口函数名（默认 `transform`/`consume`/`generate`）。

### 4.2 transform

**`map`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `expr` | map<string, expr> | 目标字段 → 表达式 |
| `project` | string[] | 仅保留这些字段 |
| `script` / `inline` | string | QuickJS 函数（等价 script 步骤的 transform 入口） |

```yaml
- { id: norm, kind: transform, type: map, config:
    { expr: { full_name: "first + ' ' + last", age: "int(age)" }, project: [full_name, age] } }
```

**`filter`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `where` | expr | 谓词，为真保留 |
| `invert` | bool | 取反 |

**`add_field`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `field` | string | 字段名 |
| `value` | any \| expr | 常量或表达式 |

**`assert`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `expect` | Assertion[] | 断言列表 |
| `on_fail` | enum | `error`(默认) \| `filter` \| `route` |

```yaml
- { id: validate, kind: transform, type: assert, config:
    { expect:
      [ { field: age, op: gte, value: 0, message: "年龄不能为负" },
        { field: email, op: matches, value: "^\\S+@\\S+$" } ],
      on_fail: error } }
```

Assertion：`{ field, op, value, message? }`；`op ∈ { eq, neq, gt, gte, lt, lte, in, contains, matches, is_null, not_null }`。

**`aggregate`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `group_by` | string[] | 分组键 |
| `reduce` | map<string, agg> | 目标字段 → 聚合 |
| `window` | { size, every? } | 可选窗口（与 batch 类似） |

`agg ∈ { count, sum, avg, min, max, first, last, collect }`。

**`merge`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `with` | string | 另一 step id 或 file 路径 |
| `on` | string[] | 合并键 |
| `how` | enum | `concat`(默认) \| `upsert` |

**`join`**（S5）：`{ with, on, type: inner|left|right|outer }`。

> **待定稿（S5）**：`with` 当前指静态文件/`inline` 数据；若需与同图内另一**活流** join，扇入时序/关联键/背压语义留待 S5 定稿。

**`split`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `by` | expr | 求值结果必须为数组，每项产出 1 条记录 |
| `as` | string | 子记录字段名（可选） |

**`batch`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `size` | int | 每批记录数（与 `within` 二选一或并存） |
| `within` | duration | 时间窗口 |
| `group_by` | string[] | 可选，键内聚合 |

**`dedup`**：`{ keys: string[] }`。

**`script`**（S5）：见 `quickjs-api.md`。

### 4.3 sink

**`file`**：`{ path, format, append?, columns?, delimiter?, header? }`。
**`db`**（S5，已实现）：`{ path, table }`——缓冲记录，`flush` 时建表（列 TEXT）并批量插入。
**`http`**（S5，已实现）：`{ url, method?, headers? }`——逐条 POST JSON。
**`queue`**（S3）：`{ topic }`。
**`log`**：`{ level?, format? }`；`format: text|json`。
**`noop`**：`{}`。
**`collect`**：`{ name? }`（测试用）。
**`script`**（S5，已实现）：`{ script | inline }`——调用 `consume(record, ctx)`。
**`email`**（延后）：`{ smtp, to, subject, template? }`。

### 4.4 control

> **控制步骤执行语义（定稿）**：`switch/foreach/loop/parallel/join/approve/delay/subflow` 是**运行时原语**，它们对「下游子图」做路由/迭代/汇聚/等待；子图递归执行**不构成主图环**——主图仍要求 DAG。`loop.while` 基于步骤 `state` 求值，每次迭代执行一轮循环体子图。

**`switch`**

```yaml
- { id: route, kind: control, type: switch, config:
    { cases: [ { when: "amount > 1000", to: big }, { when: "amount <= 1000", to: small } ],
      default: other } }
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `cases` | [{ when, to }] | 分支：命中 `when`（完整谓词）则路由到 `to` 步骤 |
| `default` | string | 默认目标步骤 |

**`foreach`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `over` | expr | 数组表达式（如 `$.items`） |
| `as` | string | 每项绑定的字段名（默认 `item`） |
| `max_iterations` | int | 上限保护 |

**`loop`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `while` | expr | 循环条件（基于步骤 state） |
| `max_iterations` | int | 必填保护 |
| `step` | string | 循环体子步骤 id |

**`parallel`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `branches` | string[] | 并发分支步骤 id |
| `concurrency` | int | 并发度（默认不限制，受背压约束） |

**`join`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `mode` | enum | `all`(默认) \| `any` \| `n` |
| `n` | int | mode=n 时所需到达数 |
| `on` | string[] | 关联键（可选） |

**`delay`**：`{ duration }`。

**`approve`**（S4）

| 字段 | 类型 | 说明 |
|---|---|---|
| `title` | string | 审批标题 |
| `owners` | string[] | 审批人 |
| `timeout` | duration | 超时（可选，超时策略 `reject|remind`） |
| `allow_edit` | bool | 是否允许修改 payload |

**`subflow`**（延后，未实现）：`{ flow: file | inline, inputs?, outputs? }`。

### 4.4.1 Schema 片段（延后，未实现）

> `Schema/DataType` 为 S5 可选增强，不进入 S0–S4 的 JSON 主路径（见 `redesign-and-roadmap.md` §2.3）。

```yaml
schema:
  columns:
    - { name: id, type: string, nullable: false }
    - { name: age, type: int, nullable: true }
```

---

## 5. edges 语法

支持三种等价写法：

```yaml
edges:
  - [in, adult]              # 列表简写
  - { from: adult, to: out } # 对象
  - { from: adult, to: out, when: "age > 18" }  # 带条件边
```

- 缺省 `edges` 时，**仅当有且只有一个 source/trigger** 才按 `steps` 声明顺序线性串联；否则必须显式声明 `edges`。
- `from`/`to` 必须引用存在的 step id；禁止自环；`run` 模式要求整体为 DAG（循环通过 `foreach/loop` 控制步骤的下游子图迭代实现，不依赖主图环）。
- 一条边可携带 `when`，等价于把条件写在目标步骤的 `when` 上。

---

## 6. 表达式语言

统一表达式子集，供 `when` / `where` / `expr` / `over` / `by` / `cases.when` 等字段使用。

| 类别 | 语法 | 示例 |
|---|---|---|
| 路径 | `$.a.b`、`$["a b"]`、`$.items[*]` | `$.user.name` |
| 比较 | `==  !=  >  >=  <  <=` | `age >= 18` |
| 逻辑 | `and  or  not` | `age >= 18 and status == "active"` |
| 算术 | `+ - * / %` | `price * qty` |
| 字符串模板 | `${...}` 在字符串内 | `"user:${name}"` |
| 函数 | `int(x) float(x) str(x) len(x) now() uuid() env(n) var(n)` | `int(age)` |
| 成员 | `in` / `matches`（正则） | `status in ["a","b"]`、`email matches "^\\S+@\\S+$"` |

**语义**：
- 表达式默认对「当前记录字段」求值，字段名直接书写（`age` 即 `$.age`）；`cases.when` 同样写完整谓词（如 `amount > 1000`），不出现无左操作数的片段。
- **优先级（定稿）**：`or` < `and` < `not` < 比较（`== != > >= < <= in matches`）< 算术（`+ -` 低于 `* / %`）< 路径/字面量/函数；可用括号显式分组。
- **类型强转（定稿）**：比较前按左侧字段类型强制转换右值；强转失败报错（计入 step 错误）。`int(x)/float(x)/str(x)` 用于显式转换。
- 复杂逻辑一律下沉到 `script` 步骤（QuickJS），不在表达式内实现语句/循环/自定义函数——保持声明式可读性。
- 表达式在 YAML 中统一用字符串书写，避免 YAML 类型歧义。

---

## 7. imports 与 templates

```yaml
# common.yaml
templates:
  normalize_email:
    kind: transform
    type: map
    config: { expr: { email: "lower(email)" } }
```

```yaml
# 主流程
imports: [common.yaml]
steps:
  - { id: e, use: normalize_email }
  - { id: out, kind: sink, type: log }
```

- `imports` 只导入 `templates`（S5 暂不支持命名步骤定义），不改变主流程的 `steps/edges`。
- `use` 引用模板；step 自身字段覆盖模板同名字段，嵌套 `config` 对象**深合并**（step 值优先）；模板可不写 `id`（以 map key 为模板名）。
- 模板解析顺序：imports 相对当前流程文件目录解析，之后与流程内联 `templates` 合并。

---

## 8. triggers（serve 模式）

```yaml
triggers:
  - { id: webhook, type: http, method: POST, path: /webhook }
  - { id: tick,    type: cron, expression: "*/5 * * * * *", timezone: UTC }
  - { id: bus,     type: queue, topic: hello }
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | string | 触发器 id（也作为图入口步骤 id） |
| `type` | enum | `http` \| `cron` \| `queue`（`db-watch`/`push` 为未来扩展，不在本路线图） |
| （type 特定） | — | `http`: method/path/auth；`cron`: expression/timezone；`queue`: topic |

- 每个触发器隐式生成一个 `source` 步骤，作为图的根节点；`edges` 中可用其 `id`。
- `run` 模式加载含 `triggers` 的文件时：忽略触发器并报提示（或 `--ignore-triggers`）。
- `serve` 模式：http 触发器把请求体/参数构造为 `Record`；cron 触发器每次到期产出 1 条 `Record`（计数经步骤 `state` 累积）；queue 触发器以消息 payload 注入。

---

## 9. 状态 / 重试 / 检查点（S4）

```yaml
steps:
  - id: count
    kind: transform
    type: map
    state: { n: 0 }
    on_error: retry
    retry: { max: 3, backoff: 1s, on: [timeout, io] }

checkpoint:
  every: 1000            # 每 N 条 或
  interval: 30s          # 每间隔（二选一/并存）
  store: .sohara/checkpoint
```

| 字段 | 说明 |
|---|---|
| `state` | 步骤累加状态初始值（对应 tiger 的 module state / rec 的 stateful） |
| `on_error` | `fail`(默认) \| `continue` \| `retry` |
| `retry.max` | 最大重试次数（仅 `on_error: retry`） |
| `retry.backoff` | 退避（固定或指数 `2x`） |
| `retry.on` | 触发重试的错误类别 |
| `checkpoint.every/interval` | checkpoint 触发条件 |
| `checkpoint.store` | checkpoint 持久化位置 |

> **状态并发（定稿）**：`state` 由运行时按步骤实例串行化（单写者/互斥），并行扇出下不产生竞态；脚本无需自行加锁。
> **投递语义（定稿）**：at-least-once；sink 可声明幂等键（record id）去重，达成「不丢、可去重」。

---

## 10. 校验规则

加载时按序校验，失败给出「文件:行/步骤 id」级别的可读错误：

1. `name`/`version` 存在且格式合法。
2. `steps` 非空；`id` 唯一且合法。
3. 每个 step 的 `kind` 合法、`type` 已注册；`config` 通过该 type 的字段校验；未知字段（含扁平简写拼写错误）**报错**（strict，见 §2）。
4. `edges` 端点存在；无自环；`run` 模式为 DAG（循环经控制步骤的下游子图实现）。
5. 缺省 `edges` 时校验「有且只有一个 source/trigger」，否则报错要求显式 `edges`。
6. 每个步骤从某 source/trigger 可达（孤儿告警）；每个步骤可达某 sink（悬空告警）。
7. `imports`/`use` 可解析；模板字段合并成功。
8. 表达式可解析（词法/语法校验，含优先级）。
9. `triggers` 仅 `serve` 模式生效；cron `expression` 合法。
10. `checkpoint`/`on_error`/`retry` 字段类型正确。

---

## 11. 完整示例

```yaml
name: order-pipeline
description: 订单 webhook → 校验 → 分支 → 富化 → 审批 → 落库
version: "1"
imports: [common.yaml]
env: [DATABASE_URL]
vars:
  high_threshold: 1000
triggers:
  - { id: webhook, type: http, method: POST, path: /orders }
steps:
  - { id: validate, kind: transform, type: assert, config:
      { expect: [ { field: amount, op: gte, value: 0 } ], on_fail: error } }

  - { id: route, kind: control, type: switch, config:
      { cases: [ { when: "amount > ${high_threshold}", to: approve } ],
        default: enrich } }

  - { id: approve, kind: control, type: approve, config:
      { title: "大额订单审批", owners: [alice], timeout: 24h } }

  - { id: enrich, kind: transform, type: script, config: { script: enrich.js } }

  - { id: to_db, kind: sink, type: db, config:
      { url: "${env:DATABASE_URL}", table: orders, upsert: true } }

edges:
  - [webhook, validate]
  - [validate, route]          # route 内部再路由到 approve / enrich
  - [approve, enrich]
  - [enrich, to_db]
checkpoint: { every: 500 }
```

---

## 12. schema 版本演进

| 版本 | 对应阶段 | 变更 |
|---|---|---|
| `"1"` | S1 | 线性 flow：`source/transform/sink` + 基础 `file/filter/map/add_field/assert/log/noop/collect` + `edges` |
| `"1"`（增量） | S2 | 增 `control` 步骤（switch/foreach/loop/parallel/join）、`batch/aggregate/merge/split/dedup`、带 `when` 的边 |
| `"1"`（增量） | S3 | 增 `triggers`、`queue` 源/汇、`delay` |
| `"1"`（增量） | S4 | 增 `state/on_error/retry/checkpoint`、`approve`、幂等键 |
| `"1"`（增量） | S5 | 增 `script/db/http` 步骤、`imports/templates/use`；`email/parquet/subflow/Schema` 延后 |
| `"2"`（未来） | — | 破坏性变更（如弃用扁平简写、重命名字段）时递增 |

> 原则：**增量追加不升版本**（向后兼容，旧文件仍可加载）；破坏性变更才升大版本并提供迁移。
