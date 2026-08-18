# Sohara Next Roadmap: Gap Closure + Ecosystem Expansion

> Status: **draft v8** (awaiting user approval)
> Date: 2026-08-17
> Depends on: `redesign-and-roadmap.md` (S0–S7 complete), `distributed-plane-and-dashboard.md` (D1–D6 complete)
> Updates: `extension-points.md` (§1 registerStep promoted to A2; §2 extension interface promoted to B)
> Sync required on approval: `quickjs-api.md` §8 (scope change from per-context to per-flow, see A2)

---

## 1. Current Status

### 1.1 Completed

| Scope | Status | Evidence |
|---|---|---|
| S0–S7 core roadmap | ✅ | 122 tests, 8 examples, release binary |
| D1–D6 distributed | ✅ | 121 tests, 3-process e2e |
| sohara-js host bridge | ✅ | 22 tests (6 script + 6 host_core + 10 host_io) |

### 1.2 Known Gaps (noop / deferred)

| Item | Current State | Root Cause |
|---|---|---|
| `ctx.checkpoint()` | Debug log only | JS bridge has no reference to runtime `persist.rs` |
| `ctx.state` persistence | In-process memory; lost on restart | `StepEnv.state` disconnected from `Executor.states` + `StateStore` |
| `sohara.registerStep()` | Not implemented | No code |
| Async bridge | Not spiked | QuickJS Promise support is synchronous polling only (see A0a) |
| Schema validation | Not implemented | S5 optional enhancement, deferred |

### 1.3 Core Disconnect

```
Runtime Executor                    JS Bridge (StepEnv)
┌─────────────────────┐            ┌──────────────────────┐
│ states: Mutex<Map>  │   ← none → │ state: Arc<Mutex>    │
│ store: StateStore   │   ← none → │ (no store ref)       │
│ checkpoint()        │   ← none → │ checkpoint() = no-op │
│ update_state()      │   ← none → │ state_sync() = mem   │
└─────────────────────┘            └──────────────────────┘
```

`BuildContext` carries no `StateStore` or checkpoint callback. `StepEnv` cannot reach the runtime's persistence layer.

---

## 2. Decisions (finalized)

| ID | Question | Decision | Rationale |
|---|---|---|---|
| OD-1 | Extension strategy | **Subprocess IPC over stdio** for third-party extensions; compile-time Rust integration remains available for in-tree components; runtime dylib plugins are deferred. | `dylib` still has ABI fragility (`dyn Trait`/Rust version coupling). A stdio boundary gives language-agnostic extensibility, crash isolation, and simpler versioning. The trade-off is IPC overhead, so v1 first targets **the main runtime surfaces**: `source` / `transform` / `sink` / `trigger` / `state-store` / `event-bus`. One explicit caveat remains: the current `EventBus` trait is publish-only, so an external `event_bus` does **not** replace the built-in queue trigger subscription path. |
| OD-6 | Builtin namespace policy | Only Sohara-shipped trusted manifests may register names beginning with `builtin-`. | Prevents third-party manifests from polluting the reserved builtin namespace while still allowing the project to ship builtin stdio extensions such as sqlite / redis / zeromq. |
| OD-2 | Extension trait granularity | One `Extension` trait with default no-op methods | Simpler; extensions self-select via empty defaults |
| OD-3 | Zero-config persistence | YAML files in data-dir | Reuses existing YAML pipeline; no new dependency |
| OD-4 | JS DSL runtime path | JS → JSON → direct `FlowConfig` via serde | Fewer moving parts; single serialization path |
| OD-5 | npm package scope | **Full SDK** (builder + types + runner + yaml) | Thin wrapper provides no value over raw CLI; the builder IS the product. But: single implementation in TypeScript, preamble.js mirrors it via tests (not dual codegen). |

---

## 3. Increments

> Each increment produces an observable result, has explicit verification, and can be executed independently (subject to stated prerequisites).

### Phase A0 — Spikes (risk reduction before commitment)

#### A0a: Async Bridge Spike

**Prerequisites**: none

**Goal**: Determine whether host-injected async operations (e.g. `sohara.http.request` returning a Promise) can work with the `quick-js` crate, or whether the sync baseline is final.

**Known constraint**: the `quick-js` crate (v0.4.1) handles Promises via **synchronous polling** (`bindings.rs:980-1039`): it injects `.then()/.catch()` handlers, then loops `JS_ExecutePendingJob` checking a global `__promiseResult` flag — **blocking the calling thread**. `JS_ExecutePendingJob` is not exposed as a public API. There is **no mechanism for Rust-side code to create or resolve a QuickJS Promise**.

**Implication**: the naive approach (spawn tokio task → resolve Promise from Rust) is not possible with the current crate. Three paths:

1. **Fork `quick-js`**: expose `JS_ExecutePendingJob` + add a `new_promise()` API that returns a resolver handle. The bridge could then create a Promise, return it to JS, spawn a tokio task, and have the task resolve the Promise via the handle. The pump loop would be called between host invocations.
2. **Thenable workaround** (no fork): host functions return a plain JS object with a `.then()` method. The preamble.js wraps it in `new Promise(resolve => obj.then(resolve))`. The host function spawns a tokio task and stores the result in a shared cell. The `.then()` callback polls the cell (busy-wait or yield via `setTimeout`). Crude but avoids forking.
3. **Accept sync baseline**: all host calls remain blocking. A3 is cancelled.

**Spike scope**: path 1 or 2, whichever is cheaper to prototype. Test with `sohara.http.request` only.

**Verification**: script `const r = await sohara.http.request({url:...})` resolves without deadlock; timeout cancels correctly.

**Complete when**: one path is validated with evidence, **or** all three are documented as infeasible/undesirable and the sync baseline is confirmed final. A1/A2 proceed regardless (sync callbacks are unaffected).

#### A0b: Stdio Extension Protocol Spike

**Prerequisites**: none

**Goal**: Verify manifest-driven subprocess extensions (`JSON-RPC` over stdio) work in practice before committing to the full B phase.

**Actions**:
1. Define a minimal manifest schema: `name`, `version`, `protocol`, `command`, `args`, `steps[]`.
2. Define newline-delimited `JSON-RPC 2.0` over stdio with `initialize`, `source.pull`, `transform`, `sink.send`, `sink.flush`, `trigger.start`, `trigger.pull`, `trigger.stop`, `state.load`, `state.save`, `state.delete`, and `bus.publish`.
3. Build a minimal external extension (any language) that proves at least one step, one trigger, one state-store, or one event-bus path.
4. Load it from the host via `--extensions`, run an end-to-end flow, and verify duplicate registrations fail clearly.

**Verification**: external steps, triggers, state-store, and event-bus paths execute with readable config/protocol failures; malformed/duplicate manifests fail cleanly.

**Complete when**: the protocol + host loading path are validated with evidence and Phase B can proceed on top of the stdio contract.

---

### Phase A — sohara-js Gap Closure

#### A1: Checkpoint + State Bridge

**Prerequisites**: none (sync callbacks regardless of A0a result)

**Design principle**: `Executor.states` is the **single authoritative copy** of step state. The JS bridge reads from and writes back to it; there is no independent `StepEnv.state`.

**State write-back strategy**: **checkpoint-throttled**, not per-record. Direct per-record writes to `JsonFileStore` rewrite the entire file each time — unacceptable for high-throughput flows. Instead:
- `state_sync()` writes back to `Executor.states` (in-memory, cheap).
- Persist to `StateStore` only on `checkpoint()` call or `checkpoint.every` counter — same model as `persist.rs::maybe_checkpoint`.

**Actions**:
1. Add to `BuildContext` (`sohara-core/src/registry.rs`):
   - `state_accessor: Option<Arc<dyn StateAccessor>>` — trait with `get(step_id) -> Value`, `set(step_id, Value)`, `checkpoint()` methods.
2. Define `StateAccessor` trait in `sohara-core/src/store.rs` (3 methods, all sync).
3. In `sohara-runtime`, implement `StateAccessor` for `Executor` — `get`/`set` operate on `self.states`; `checkpoint` calls `self.checkpoint()`.
4. Inject into `BuildContext` when constructing the executor.
5. `StepEnv`: replace `state: Arc<Mutex<Value>>` with `state_accessor: Option<Arc<dyn StateAccessor>>` + `step_id: String`.
6. `host.rs`:
   - `state_sync()`: writes to `state_accessor.set(step_id, value)` (in-memory only).
   - `checkpoint()`: calls `state_accessor.checkpoint()` (triggers persist).
7. `bridge.rs`: `state_js()` reads from accessor; remove `initial_state()` (no more independent state).
8. Tests: verify state persists to `StateStore` on checkpoint; verify state survives flow restart with `--resume`.

**Constraint**: `StateAccessor::set` acquires `Executor.states`'s `Mutex`. This call must not occur while the executor holds the same lock (e.g. inside `eval_value` or `update_state`). Current code paths are safe — `state_sync` runs in the JS callback chain, which is outside the executor's walk loop — but this invariant must be preserved. Document it in the implementation with a `// SAFETY: must not be called while executor holds states lock` comment.

**Verification**: `cargo test --workspace` passes; script `ctx.state.count = (ctx.state.count || 0) + 1` across records → checkpoint → StateStore contains accumulated value; `--resume` restores state.

**Complete when**: single state authority works; checkpoint throttling works; 3+ new tests pass.

#### A2: `sohara.registerStep()`

**Prerequisites**: none (independent of A1)

**Scope model**: **per-flow** (not per-context). Rationale: per-context (as currently described in `quickjs-api.md` §8) makes registered steps invisible to YAML — the whole point of registration is to make script-defined steps usable in the flow's `ComponentRegistry`. Per-flow is the useful model.

**Sync required**: on approval, update `quickjs-api.md` §8 to reflect per-flow scope and build-time registration semantics.

**Build ordering**: YAML steps are built in declaration order. A step `type: slugify` can only resolve if the script defining it was built first. Solution: **two-pass build**.
- Pass 1: build all `script`-type steps, collect `registerStep` registrations into the `ComponentRegistry`.
- Pass 2: build remaining steps (which can now resolve registered types).

**Registration passing**: `StepFactory::build` returns `BuiltStep` — there is no channel for registration metadata. Fix: `BuildContext` gains `registered_steps: Arc<Mutex<Vec<RegisteredStep>>>` (a new struct holding `kind`, `type`, `script_source`, `handler_name`). `ScriptStep::build` populates this via the `__register_step` callback. After Pass 1, `build_flow` drains the vec and injects synthetic `StepFactory` entries into the `ComponentRegistry` before Pass 2.

**Actions**:
1. `sohara-core/src/registry.rs`: add `RegisteredStep` struct and `BuildContext.registered_steps` field.
2. `preamble.js`: `sohara.registerStep(kind, type, fn)` stores into `__registeredSteps` array.
3. `host.rs`: new callback `__register_step(kind, type, handler_name)` pushes to `ctx.registered_steps`.
4. `sohara-config/src/build.rs`: modify `build_flow` to two-pass — first build script steps (populating `registered_steps`), then drain registrations into synthetic factories, then build remaining steps.
5. Registered step factory: creates a `ScriptStep`-like wrapper that holds the script source + handler name, executes top-level + entry on each invocation (same as existing `ScriptStep`).
6. Lifecycle: registrations collected at build time; handler executes per-invocation with a fresh QuickJS context (same isolation model as `ScriptStep`).

**Verification**: script defines `sohara.registerStep("transform", "slugify", fn)` → YAML step `type: slugify` executes correctly in the same flow.

**Complete when**: two-pass build works; registered step runs; test passes.

#### A3: Async Bridge (conditional on A0a)

**Prerequisites**: A0a succeeds

**If A0a succeeds**: implement the async bridge using the validated mechanism (non-blocking host calls + microtask pump). This changes A1's callback signatures — `state_fn` and `checkpoint_fn` must return Futures.

**If A0a fails**: A3 is cancelled. Document the infeasibility evidence in `quickjs-api.md` §7. The sync baseline is final.

---

### Phase B — External Extensions (subprocess IPC primary)

#### B1: Manifest + JSON-RPC Contract

**Prerequisites**: A0b

**Actions**:
1. Define manifest schema (`.yaml`/`.json`): root `command/args`, `protocol`, and declared `(kind, type)` step registrations.
2. Define newline-delimited `JSON-RPC 2.0` messages:
   - `initialize` → returns `{ protocol, capabilities }`
   - `source.pull` → returns `{ records, done }`
   - `transform` → returns `{ outcome: pass|filtered|expand, ... }`
   - `sink.send` / `sink.flush` → ack or structured error
   - `trigger.start` / `trigger.pull` / `trigger.stop`
   - `state.load` / `state.save` / `state.delete`
   - `bus.publish`
3. Document host guarantees:
   - one child process per built external step instance
   - one child process per built external trigger instance
   - one child process per built external state-store or event-bus provider instance
   - one in-flight request at a time per child
   - stdout reserved for protocol frames; extension logs go to stderr
4. Explicit v1 scope: **source / transform / sink / trigger / state-store / event-bus**.
5. Preserve the current architectural caveat: external `event_bus` is publish-only and therefore does not replace the built-in `queue` trigger subscription model.

**Verification**: example extension round-trips step + trigger + provider methods; protocol fixture test passes.

**Complete when**: the protocol is stable enough for host + example extension development.

#### B2: Host Loader + Runtime Registration

**Prerequisites**: B1

**Actions**:
1. `sohara-runtime`: add `load_stdio_extensions(registry, paths)`:
   - accept manifest files or directories
   - parse + validate manifests
   - reject duplicate `(kind, type)` registrations
   - register synthetic `StepFactory` entries into `ComponentRegistry`
   - expose external trigger/state-store/event-bus builders to runtime/CLI
2. CLI: add `--extensions PATH` to `run`, `serve`, and `approve`.
3. Flow config integration:
   - `checkpoint.store` accepts string path **or** external provider object
   - top-level `event_bus` accepts an external provider object
   - `serve` consults the extension host for unknown trigger types
4. Reserve `builtin-*` names for trusted Sohara manifests only; reject third-party manifests that attempt to claim them.
5. Auto-load trusted builtin manifests from the repository's builtin extension directory before user-provided `--extensions`.
6. Resolve relative `command` paths against the manifest location.
7. Log loaded extensions at startup: `"loaded stdio extension: {name} v{version}"`.

**Verification**: `sohara run/serve ... --extensions ./extensions` loads the manifest and executes external step/trigger/provider paths.

**Complete when**: loading + registration works in CLI and integration tests pass.

#### B3: First Cross-Language Extension Example

**Prerequisites**: B1

**Actions**:
1. Ship a tiny external example set plus trusted builtin providers. The builtin set should be **Rust implementations** (e.g. sqlite / redis / zeromq), even if third-party extensions remain language-agnostic.
2. Provide a manifest + executable example under `examples/` or a dedicated `extensions/` folder.
3. Document how the extension receives `step`, `config`, and `record`.
4. Use that example as the reference contract while timeout/restart policy and richer bus semantics are still evolving.

**Verification**: example extension works end-to-end from CLI without rebuilding the `sohara` binary.

**Complete when**: one external extension proves the language-agnostic path.

> Stateful backends (redis/postgres/kafka/etc.) remain follow-on work once the generic trigger/state-store/event-bus IPC contract has stabilized and durability / backpressure / subscription semantics are better specified.

---

### Phase C — Zero Config Bootstrap

#### C1: `sohara server` Command (empty startup only)

**Prerequisites**: none

**Actions**:
1. `sohara-cli/src/main.rs`: add `Server` subcommand with `--addr`, `--admin-token` (required), `--data-dir`, `--extensions`.
2. On start: load extensions, start admin API (`/admin/health`), **load no flows**.
3. `--admin-token` is **mandatory** for `server` subcommand (the API surface is a remote code execution vector — any `POST /api/flows` can declare `script` steps with `allow: [http, file.write]`).

**Verification**: `sohara server --addr 127.0.0.1:9530 --admin-token test` starts; `GET /admin/health` returns 200; no token → request rejected.

**Complete when**: server starts empty; health + auth work; no flows loaded.

#### C2: Flow Management API

**Prerequisites**: C1

**Security model**: all `/api/flows/*` endpoints require `Authorization: Bearer <admin-token>` (same middleware as existing `/admin/*`).

**Key design**:
- `id` and `name` are the same value (the `name` field from `FlowConfig`). This avoids a second key namespace and the `id` vs `name` ambiguity.
- Filenames: sanitize `name` to `[a-z0-9_-]` only; reject names with path separators or `..`.

**Plane/agent compatibility**: the existing model is "1 instance = 1 flow = 1 process" (`sohara-agent` + `sohara-plane`). `sohara server` introduces "1 process = N flows". These coexist:
- `sohara server` is for single-machine, API-driven deployments.
- `sohara-agent` + `sohara-plane` remains for multi-instance, declarative deployments.
- They do not conflict because they target different deployment topologies. No migration needed.

**Actions**:
1. New module `sohara-runtime/src/flow_manager.rs`:
   - `FlowManager` holds `HashMap<String, FlowInstance>` (key = flow name).
   - Each `FlowInstance`: own `Executor`, `InProcessBus`, triggers, status, tokio task handle.
   - Status: `created | running | stopped | error`.
2. New admin endpoints in `admin.rs`:
   - `POST /api/flows` — body: YAML string; parse → validate → create; returns `{ name, status: "created" }`.
   - `GET /api/flows` — list all with status.
   - `GET /api/flows/:name` — detail + status + stats.
   - `PUT /api/flows/:name` — update (stop → replace → restart).
   - `DELETE /api/flows/:name` — stop + remove.
   - `POST /api/flows/:name/start` — start.
   - `POST /api/flows/:name/stop` — stop.
3. All endpoints require Bearer token auth.
4. Name validation: `[a-z0-9][a-z0-9_-]*`, max 64 chars, no path separators.

**Verification**: `POST /api/flows` with YAML → `GET /api/flows` lists it → `POST /api/flows/:name/start` → `GET /api/flows/:name/status` shows `running`. Unauthorized request → 401.

**Complete when**: CRUD + lifecycle + auth work; 4+ integration tests pass.

#### C3: Flow Persistence + Auto-Recovery

**Prerequisites**: C2

**Actions**:
1. On `POST /api/flows` (create) and `PUT /api/flows/:name` (update): persist YAML to `<data-dir>/flows/<name>.yaml`.
2. On `DELETE`: remove the file.
3. On status change (`running`/`stopped`): update `<data-dir>/state.json`.
4. On server boot: scan `<data-dir>/flows/`, read `state.json`, auto-start flows that were `running` at last shutdown.

**Verification**: create flow via API → kill server → restart → flow auto-starts.

**Complete when**: survive restart; test passes.

---

### Phase D — JS API DSL

#### D1: Flow Builder DSL (build + save only)

**Prerequisites**: none

**Actions**:
1. `preamble.js`: add `FlowBuilder` class with chainable methods:
   - `sohara.flow(name)` → `FlowBuilder`
   - `.source(id, type, config)`, `.transform(...)`, `.sink(...)`, `.control(...)`
   - `.edge(from, to)`, `.trigger(id, type, config)`
   - `.vars(obj)`, `.checkpoint(config)`
   - `.build()` → returns plain JS object (`{ name, version, steps, edges, triggers, vars, checkpoint }`)
2. `host.rs`: new callback `__flow_save(config_json, path)` — serializes JSON config to YAML file.
3. Exposed as `sohara.flow.save(builderResult, path)`.

**Verification**: `sohara.flow("x").source(...).sink(...).build()` returns correct JSON; `sohara.flow.save(result, "/tmp/test.yaml")` writes valid YAML that `sohara run` can execute.

**Complete when**: builder + save work; round-trip test passes (build → save → `sohara run`).

#### D2: Runtime Interpretation of JS-Defined Flows

**Prerequisites**: D1

**Thread model**: `__flow_run` spawns the sub-flow in an **independent tokio runtime** on a dedicated OS thread (same pattern as `host::http_request` — avoids worker starvation in the parent runtime). Returns a stop handle to the script.

**Stop semantics**: `sohara.flow.run(config)` returns a control object:
```js
const handle = sohara.flow.run(config);
// ... later ...
handle.stop();  // triggers graceful shutdown of the sub-flow
```

For **run-style** flows (finite): `run()` blocks until the flow completes naturally.
For **serve-style** flows (infinite): `run()` returns immediately with a stop handle; the script must call `handle.stop()` or the flow runs until the parent context is destroyed.

**No C2 dependency**: D2 runs sub-flows directly via `Executor`, not through `FlowManager`. This keeps D2 self-contained.

**Actions**:
1. `sohara-config`: make `FlowConfig::from_json_value(Value) -> Result<FlowConfig>` public.
2. `host.rs`: new callback `__flow_run(config_json)` — spawns a tokio runtime on a new OS thread, runs the flow, returns a stop handle object.
3. `preamble.js`: `sohara.flow.run(builderResult)` returns `{ stop() }` handle.
4. Cleanup: when the parent script context is dropped, any running sub-flows are stopped.

**Verification**: script defines flow via DSL → `sohara.flow.run(result)` → records flow through → `handle.stop()` stops it cleanly.

**Complete when**: run-style flow completes; serve-style flow starts + stops via handle; test passes.

---

### Phase E — Node.js/npm Development Workflow

#### E1: `@sohara/sdk` npm Package

**Prerequisites**: D1

**Single implementation**: the `FlowBuilder` in TypeScript is the **source of truth**. The preamble.js version mirrors it, validated by a shared test suite that asserts both produce identical JSON for the same inputs. No dual codegen.

**Binary distribution**: the SDK's `runner.ts` requires `sohara` in PATH. On first run, if `sohara` is not found, print a clear error with install instructions (download from GitHub Releases, or `cargo install sohara-cli`). No auto-download for now — document this as a known limitation; add `optionalDependencies` per-platform binary packages as a future enhancement.

**Actions**:
1. Create `packages/sohara-sdk/` with `package.json`, `tsconfig.json`.
2. `src/builder.ts`: TypeScript `FlowBuilder` class.
3. `src/yaml.ts`: serialize to YAML via `js-yaml`.
4. `src/runner.ts`: `run(config)` / `serve(config)` — spawn `sohara` from PATH; fail with clear error if not found.
5. `src/types.ts`: TypeScript types.
6. `src/index.ts`: re-export.
7. Tests: shared JSON fixtures with preamble.js tests.

**Verification**: `npm test` passes; `FlowBuilder` output matches preamble.js; YAML round-trips through `sohara-config`.

**Complete when**: package builds, tests pass, types are correct.

#### E2: `sohara-dev` Development Server

**Prerequisites**: E1

**Actions**:
1. `packages/sohara-sdk/src/dev.ts`: watch `.ts`/`.js` → recompile → restart `sohara serve`.
2. Uses `chokidar`; spawns `sohara serve` as child process; SIGTERM + restart on change.
3. `npx sohara-dev [--flow flow.ts] [--port PORT]`.

**Verification**: `npx sohara-dev` starts; edit `flow.ts` → server restarts.

**Complete when**: watch + restart cycle works.

#### E3: Project Scaffolding

**Prerequisites**: E1

**Actions**:
1. `packages/create-sohara-app/`: `npx create-sohara-app my-project`.
2. Template: `flow.ts`, `steps/`, `package.json`, `tsconfig.json`, `README.md`.
3. `npm run build` → `dist/flow.yaml`; `npm run dev` → `sohara-dev`; `npm run start` → `sohara serve`.

**Verification**: scaffolded project builds and `sohara run dist/flow.yaml` works.

**Complete when**: scaffold → build → run end-to-end.

---

## 4. Execution Order

```
A0a (async spike)  ──── no gate on A1 ────► determines A3 viability
A0b (stdio spike)  ──────────────────────► validates B strategy

A1 (checkpoint+state)  ──┐
A2 (registerStep)      ──┤  (all independent)
A3 (async bridge)      ──┘  (conditional on A0a success)

B1 (protocol)    [after A0b] ──► B2 (loader) ──► B3 (example extension)

C1 (server) ──► C2 (flow API) ──► C3 (auto-recovery)

D1 (DSL build+save) ──► D2 (runtime interp)

E1 (sohara-sdk) ──► E2 (sohara-dev)
                └──► E3 (scaffolding)
```

**Recommended sequence**: A0a ∥ A0b ∥ A1 ∥ A2 → B1 → B2 → C1 → D1 → C2 → D2 → E1 → B3 → C3 → E2 → E3 → A3 (if applicable)

Each step is independently committable and verifiable.

---

## 5. Exclusions

- Distributed execution (already complete D1–D6)
- AI agent integration (`extension-points.md` remaining anchors)
- Schema/DataType validation layer
- Prometheus metrics export
- Parquet format support
- Email sink / join transform / subflow nesting
- Dynamic `registerStep` across flows (flow-scoped only)
- Async bridge as production feature (A3 conditional; sync baseline is the fallback)
- Auto-download of `sohara` binary in npm packages (documented limitation)

---

## 6. Assumptions

1. Third-party extensions are external child processes launched from manifest files over stdio; users do **not** rebuild the `sohara` binary to add them. Runtime dylib loading is deferred.
2. `ctx.checkpoint()` callbacks use `Arc<dyn StateAccessor>` (sync) — matches the synchronous JS bridge baseline.
3. Zero-config server uses YAML files in data-dir for persistence. `state.json` (JSON) tracks runtime status — dual format is acceptable because YAML is the source of truth for flow definitions and JSON is ephemeral runtime state.
4. JS DSL produces JSON that serde deserializes into `FlowConfig`.
5. `sohara` binary must be pre-installed and in PATH for the npm SDK. No auto-download mechanism in v1.
6. The v1 stdio host supports **source / transform / sink / trigger / state-store / event-bus**. The preserved limitation is that external `event_bus` is publish-only and therefore not a drop-in replacement for the built-in `queue` trigger subscription path.

---

## 7. Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A0a async spike fails → sync baseline final | Medium | Low (sync already works) | Document infeasibility; proceed with sync |
| A0b stdio spike reveals unacceptable protocol or lifecycle complexity | Low | Medium | Reduce scope to a single data-step kind first; defer richer extension lifecycle |
| Child process crash / hang leaks capacity or stalls a step | Medium | Medium | Startup handshake, explicit protocol errors, per-request timeout/restart as follow-up increment |
| IPC overhead is too high for hot-path data steps | Medium | Medium | Keep stdio for extensibility, but recommend in-process Rust implementations for latency-critical paths |
| External `event_bus` is mistaken for a full queue-trigger replacement | Medium | Medium | Preserve and document the conflict: publish-only bus support is implemented, but `queue` trigger still requires the built-in in-process bus |
| Third-party manifests impersonate builtin extensions | Medium | Medium | Reserve `builtin-*` for trusted builtin manifests and reject those names from untrusted paths |
| A1 state write-back introduces performance regression | Low | Medium | Benchmark high-throughput flows; checkpoint throttling prevents per-record I/O |
| A2 two-pass build introduces ordering complexity | Low | Low | Well-defined algorithm; test with mixed script + non-script steps |
| C2 remote code execution surface | Medium | High | Mandatory `--admin-token`; name sanitization; no anonymous access |
| D2 sub-flow runtime resource leak | Medium | Medium | Stop handle + parent context drop cleanup; test with serve-style flows |
| E1 `sohara` not in PATH → Phase E entirely unusable | High | High | Clear error message with install instructions; document as known v1 limitation; consider `optionalDependencies` per-platform binaries as future enhancement |

---

## 8. Overall Completion Criteria

This roadmap is complete when:

1. **Phase A**: `ctx.checkpoint()` and `ctx.state` persist through `StateStore` (single authority, checkpoint-throttled); `sohara.registerStep()` works per-flow with two-pass build; async spike resolved (one of fork/thenable/sync-baseline chosen with evidence).
2. **Phase B**: manifest + stdio JSON-RPC contract exists; host loader registers external source/transform/sink steps plus trigger/state-store/event-bus providers from `--extensions`; trusted builtin manifests can claim `builtin-*` names while untrusted ones cannot; at least one shipped builtin extension works end-to-end without rebuilding `sohara`, with the queue-trigger/external-bus limitation explicitly preserved.
3. **Phase C**: `sohara server` starts empty; flow CRUD + lifecycle via API; survives restart with auto-recovery; all endpoints authenticated.
4. **Phase D**: JS DSL can define, save, and run flows programmatically; sub-flows have proper stop semantics.
5. **Phase E**: `@sohara/sdk` npm package builds, tests pass; scaffolding produces a runnable project.

Each phase is independently deliverable. The roadmap is approved phase-by-phase — later phases may be revised based on earlier phase learnings.

---

## 9. Change Log

| Version | Date | Changes |
|---|---|---|
| v1 | 2026-08-17 | Initial draft |
| v2 | 2026-08-17 | Challenge revision: FFI → static linking; OD 表对齐; state single authority; A0 spikes前置; D2 thread model; C security; E binary strategy |
| v3 | 2026-08-17 | Self-challenge + vnv: A0a rewritten (quick-js crate constraint); OD-1 clarified (compile-time features, not runtime plugins); A1 deadlock constraint added; A2 registration passing mechanism defined; E1 risk impact corrected; A1 prerequisite decoupled from A0a |
| v4 | 2026-08-17 | User decision: dylib deferred, subprocess IPC / JSON-RPC over stdio promoted; Phase B rewritten around manifest + stdio host; execution order / assumptions / risks updated to transform-only external extensions |
| v5 | 2026-08-17 | Phase B further advanced from transform-only to source/transform/sink host support; protocol methods, assumptions, risks, and completion criteria aligned to the broader data-step scope |
| v6 | 2026-08-18 | Phase B expanded to trigger/state-store/event-bus host support; flow config integration and the publish-only external event-bus caveat documented explicitly |
| v7 | 2026-08-18 | Builtin namespace policy added (`builtin-*` reserved for trusted Sohara manifests); trusted builtin sqlite/redis/zeromq extension set introduced as validation targets |
| v8 | 2026-08-18 | Builtin direction corrected: builtin extensions are Rust executables rather than repository scripts; builtin manifests now target Rust binaries, with sqlite validated and redis/zeromq present as Rust builtin providers |
