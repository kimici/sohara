# Sohara Extension Points

> Status: active
> Parts of this document have been promoted to the next roadmap — see [`next-roadmap.md`](next-roadmap.md).
> This document retains design anchors for items still outside the current scope.

---

## Promoted to Next Roadmap

The following items are now tracked in [`next-roadmap.md`](next-roadmap.md) with concrete increments:

| Item | Former Section | Next Roadmap |
|---|---|---|
| Script step registration (`sohara.registerStep`) | §1 | Phase A2 |
| Extension interface (subprocess stdio extensions; dylib deferred) | §2 (partial) | Phase B1–B3 |
| Zero-config bootstrap | — | Phase C1–C3 |
| JS API DSL for flow definition | — | Phase D1–D2 |
| Node.js/npm workflow | — | Phase E1–E3 |

---

## Remaining Design Anchors (out of scope)

> Note: the current roadmap now prioritizes **subprocess IPC / JSON-RPC over stdio** for third-party extensions, and the active host slice now covers `source` / `transform` / `sink` / `trigger` / `state-store` / `event-bus`. The one preserved caveat is that external `event_bus` is still publish-only, so it does not replace the built-in `queue` trigger subscription path. In addition, the `builtin-*` namespace is reserved for trusted Sohara-shipped manifests only. Any future dylib/plugin ABI work remains a deferred design topic, not the active implementation path.

### 1. AI Agent Interface

Not in any current roadmap. Design anchor (aligned with tiger's agent concept):

- Agent = a **special script step** with tools and memory; host bridge injects `sohara.llm.*`:
  - `sohara.llm.complete(prompt, opts)` / `sohara.llm.chat(messages, opts)`: model calls.
  - Tools exposed as function signatures (structured output → validation → error feedback).
- Guardrails (required when implemented): call budget (token/count/duration), timeout + retry, approve step for agent output review, audit log (prompt/response in run history).
- Explicitly excluded: autonomous agent orchestration (multi-agent collaboration, goal loops) — the framework provides "LLM call + tool bridge" primitives; orchestration stays declarative via flows.

### 2. Other Candidates (unsorted)

| Direction | Notes | Dependency |
|---|---|---|
| Schema/DataType enhancement | Optional schema + validation layer for `Record` (S5 deferred) | No evidence of demand |
| Parquet / text formats | `file` source/sink format extensions | Optional arrow/parquet dependency |
| Prometheus text metrics | `/admin/metrics` text/plain output | S6 deferred |
| Step/trigger disable | Fine-grained admin API toggle | Admin state model extension |
| email sink / join transform / subflow | S5 trimmed types | Per-type factory + tests |
