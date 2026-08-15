# Sohara

A lightweight single-machine automation framework written in Rust — declarative YAML workflows, event-driven triggers, streaming data pipelines, persistence/recovery, human-in-the-loop approval, and scripting.

Sohara merges concepts from [tiger](https://github.com/tiger-server/tiger) (event-driven webhook/cron/queue processing) and [rec-core](https://github.com/rec-framework/rec-core) (streaming data pipelines) into one unified model.

> **Design & roadmap**: [`docs/design/redesign-and-roadmap.md`](docs/design/redesign-and-roadmap.md) · **YAML schema**: [`docs/design/yaml-workflow-schema.md`](docs/design/yaml-workflow-schema.md) · **QuickJS API**: [`docs/design/quickjs-api.md`](docs/design/quickjs-api.md) · **Extension points**: [`docs/design/extension-points.md`](docs/design/extension-points.md) · **Distributed plane & dashboard (design)**: [`docs/design/distributed-plane-and-dashboard.md`](docs/design/distributed-plane-and-dashboard.md)

## Features

- **Declarative YAML workflows** — sources, transforms, sinks, and control steps (`switch` / `foreach` / `loop` / `parallel` / `join` / `batch` / `delay` / `state` / `approve`) wired as a DAG; run once or serve forever.
- **Triggers** — HTTP webhooks, cron schedules, and an in-process message bus, with graceful shutdown.
- **Expression language** — `when`/`where`/`expr` predicates over record fields (`age >= 18`, `int(x)`, `now()`, `env(NAME)`, `var(name)`).
- **Persistence & recovery** — checkpointed state, idempotent re-delivery (`--resume`), and human-in-the-loop approval (`sohara approve`).
- **Connectors** — files (CSV/JSON/JSONL), SQLite (bundled), and an HTTP client source/sink; extensible via a `(kind, type)` component registry.
- **Scripting** — QuickJS script steps (`source.script` / `transform.script` / `sink.script`) with a `sohara` host bridge.
- **Reusable fragments** — `imports` + `templates` + `use` with deep config merging.
- **Observability & admin (single machine)** — per-step statistics, run history (`sohara history`), and a serve-mode admin API (`/admin/health|pause|resume|metrics`).

## Quick Start

### CLI

```console
$ cargo run -p sohara-cli -- init demo && cd demo
$ cargo run -p sohara-cli -- run flow.yaml
Flow 'basic' finished: processed=2, filtered=1, errors=0, waiting=0, duplicates=0
```

A minimal flow (`flow.yaml`):

```yaml
name: basic
version: "1"
steps:
  - { id: in, kind: source, type: file, config: { path: data/input.csv, format: csv } }
  - { id: adult, kind: transform, type: filter, config: { where: "age >= 18" } }
  - { id: out, kind: sink, type: file, config: { path: output/result.jsonl, format: jsonl } }
```

### Commands

| Command | Purpose |
|---|---|
| `sohara run <flow.yaml> [--resume] [--verbose] [--history PATH]` | Run a flow once; `--verbose` prints per-step statistics; runs are recorded to `.sohara/history.jsonl` |
| `sohara serve <flow.yaml> [--admin ADDR] [--admin-token T] [--resume]` | Run triggers until Ctrl+C/SIGTERM; `--admin` enables the admin API + embedded dashboard (`/admin/ui`, status/errors/approvals/history), optionally token-protected |
| `sohara approve <flow.yaml> [--step ID]` | Approve records parked by `approve` steps |
| `sohara history [--limit N] [--history PATH]` | Show recent runs (successful and failed) |
| `sohara init [dir]` | Scaffold `flow.yaml` + `data/input.csv` |

### Rust API

```rust
use sohara_core::{Pipeline, Record, Transform, VecSource};

#[tokio::main]
async fn main() -> sohara_core::Result<()> {
    let source = VecSource::new("input", vec![
        Record::from_json(serde_json::json!({"name": "Alice", "age": 30})),
        Record::from_json(serde_json::json!({"name": "Bob", "age": 15})),
    ]);
    let transforms: Vec<Box<dyn Transform>> = vec![];
    let sink = sohara_core::LogSink::new("output");
    let stats = Pipeline::new("example").run(&source, &transforms, &sink).await?;
    println!("processed={} filtered={} errors={}", stats.processed, stats.filtered, stats.errors);
    Ok(())
}
```

## Examples

Every example is runnable from the repository root; see [`examples/README.md`](examples/README.md) for the index.

| Example | Stage | What it shows |
|---|---|---|
| `examples/basic.yaml` | S1 | csv → filter → map → jsonl |
| `examples/graph.yaml` | S2 | DAG with switch/parallel/join/foreach/batch |
| `examples/serve.yaml` | S3 | http + cron triggers in serve mode |
| `examples/approve.yaml` | S4 | human-in-the-loop approval + checkpoint store |
| `examples/script.yaml` | S5 | QuickJS transform with the `sohara` bridge |
| `examples/imports.yaml` | S5 | template fragments via `imports`/`use` |
| `examples/db.yaml` / `examples/db-read.yaml` | S5 | SQLite sink + source round-trip |

## Architecture

```
┌─────────────┐     ┌─────────────────────┐     ┌─────────────┐
│   Triggers  │ ──▶ │  FlowGraph +        │ ──▶ │    Sinks    │
│  http/cron/ │     │  Executor (DAG walk │     │ file/db/http│
│  queue      │     │  + control nodes)   │     │ log/queue   │
└─────────────┘     └─────────────────────┘     └─────────────┘
                          │        │
                 ┌────────┘        └─────────┐
                 ▼                           ▼
          StateStore (checkpoint/    QuickJS script steps
          resume/approve queue)      (sohara host bridge)
```

- Records flow as JSON values with at-least-once delivery and idempotency keys on resume.
- Control steps (`switch`/`foreach`/`loop`/`parallel`/`join`/`batch`) are runtime primitives; the main graph stays a DAG.
- Backpressure = bounded channels; serve mode drains in flight and flushes on shutdown.
- Admin pause (S6) is cooperative: while paused the executor holds the pulled record and stops taking more, so backpressure propagates upstream.

## Project Structure

| Crate | Responsibility |
|---|---|
| `sohara-core` | Data model (`Record` = JSON payload), `Source`/`Transform`/`Sink`/`Trigger`/`StateStore` traits, expression language, component registry |
| `sohara-config` | YAML schema v1, validation, imports/templates, pipeline building |
| `sohara-builtins` | Built-in steps: file/inline, filter/map/add_field/assert, control, log/noop/collect/queue |
| `sohara-runtime` | Flow graph builder + executor, serve mode, persistence/recovery, pause gate + admin API |
| `sohara-triggers` | HTTP (axum), cron, and in-process queue triggers |
| `sohara-persistence` | `StateStore` implementations: in-memory and atomic JSON file |
| `sohara-io` | Connectors: minimal HTTP/1.1 client, SQLite (bundled rusqlite) |
| `sohara-js` | QuickJS bridge + `script` source/transform/sink |
| `sohara-cli` | `sohara` binary: run / serve / approve / history / init |
| `sohara-agent` | Node agent (D2): supervises local `sohara serve` instances, heartbeat + command transport |
| `sohara-plane` | Control plane (D3–D5a): registry + reconciliation, manager API, gateway (round-robin/hash routing, health eviction), and the cross-instance relay mailbox |

## Status

S0–S6 of the [roadmap](docs/design/redesign-and-roadmap.md) are implemented and verified (94 tests, clippy-clean, length-gated); S7 (docs / examples / CI / release packaging) is in progress. Single-machine by design; distribution, AI agents, and a standalone `sohara-server` are documented future extension points.

## Development

```console
$ cargo test --workspace
$ cargo clippy --all-targets
$ cargo fmt --check
$ bash scripts/check-file-length.sh && python3 scripts/check-fn-length.py
```

Note: when the sandbox blocks `~/.cargo`, export `CARGO_HOME=$PWD/.cargo-home` first.

## License

MIT OR Apache-2.0
