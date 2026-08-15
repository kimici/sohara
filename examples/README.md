# Examples

All examples run from the repository root (paths are resolved relative to each
flow file, and outputs land in `examples/output/`).

| Example | Stage | Command | Shows |
|---|---|---|---|
| `basic.yaml` | S1 | `sohara run examples/basic.yaml` | file source → filter → map → jsonl sink |
| `graph.yaml` | S2 | `sohara run examples/graph.yaml` | DAG: switch → parallel → join → foreach → batch |
| `serve.yaml` | S3 | `sohara serve examples/serve.yaml` | http + cron triggers, graceful shutdown |
| `approve.yaml` | S4 | `sohara run examples/approve.yaml` then `sohara approve examples/approve.yaml` | human-in-the-loop approval + checkpoint store |
| `script.yaml` | S5 | `sohara run examples/script.yaml` | QuickJS transform using the `sohara` host bridge |
| `imports.yaml` | S5 | `sohara run examples/imports.yaml` | template fragments from `parts/common.yaml` via `imports`/`use` |
| `db.yaml` | S5 | `sohara run examples/db.yaml` | inline records → SQLite sink |
| `db-read.yaml` | S5 | `sohara run examples/db-read.yaml` | SQLite source → jsonl (reads the table written by `db.yaml`) |

## Serve-mode admin API (S6)

```console
$ sohara serve examples/serve.yaml --admin 127.0.0.1:9528
$ curl http://127.0.0.1:9528/admin/health
$ curl -X POST http://127.0.0.1:9528/admin/pause      # hold intake
$ curl -X POST http://127.0.0.1:9528/admin/resume     # continue
$ curl http://127.0.0.1:9528/admin/metrics            # run report JSON
```

## Run history (S6)

```console
$ sohara run examples/basic.yaml
$ sohara history --limit 5
```

Input data lives in `examples/data/`; generated outputs are written to
`examples/output/` and the SQLite file `examples/output/demo.db` is created on
first `db.yaml` run.
