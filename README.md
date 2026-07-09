# Sohara

A lightweight event-driven data processing framework written in Rust.

Sohara combines the concepts from [tiger-server](https://github.com/tiger-server/tiger) (event-driven webhook/cron/queue processing) and [rec-core](https://github.com/rec-framework/rec-core) (streaming data processing) into a unified framework.

## Features

- **Event-driven triggers**: HTTP webhooks, cron jobs, message queues, database subscriptions, push notifications
- **Streaming data processing**: Process records through composable pipelines
- **Transform/Filter/Aggregate**: Built-in data transformation primitives
- **QuickJS integration**: Extensible via JavaScript scripting
- **Multiple sinks**: File, database, HTTP, queue, email output

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Triggers  │ ──▶ │  Pipeline   │ ──▶ │    Sinks    │
│ (Sources)   │     │ (Transforms)│     │   (Output)  │
└─────────────┘     └─────────────┘     └─────────────┘
     │                    │                    │
     │                    │                    │
  ┌──┴──┐            ┌───┴───┐            ┌───┴───┐
  │HTTP │            │Filter │            │ File  │
  │Cron │            │Map    │            │ DB    │
  │Queue│            │Merge  │            │ HTTP  │
  │DB   │            │Aggregate           │ Queue │
  │Push │            └───────┘            │ Email │
  └─────┘                                 └───────┘
```

## Quick Start

```rust
use sohara_core::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a pipeline
    let pipeline = Pipeline::new("example");
    
    // Create a source with some records
    let source = VecSource::new("input", vec![
        Record::from_json(serde_json::json!({"name": "Alice", "age": 30})),
        Record::from_json(serde_json::json!({"name": "Bob", "age": 25})),
    ]);
    
    // Create transforms
    let transforms: Vec<Box<dyn Transform>> = vec![
        Box::new(MapTransform::new("add-timestamp", |mut record| {
            record.set("processed_at", serde_json::json!(chrono::Utc::now().to_rfc3339()));
            record
        })),
    ];
    
    // Create a sink
    let sink = LogSink::new("output");
    
    // Run the pipeline
    let stats = pipeline.run(&source, &transforms, &sink).await?;
    println!("Processed: {}, Filtered: {}, Errors: {}", 
        stats.processed, stats.filtered, stats.errors);
    
    Ok(())
}
```

## Project Structure

```
sohara/
├── sohara-core/      # Core abstractions (Source, Sink, Transform, Pipeline, Record)
├── sohara-trigger/   # Trigger implementations (HTTP, Cron, Queue, DB, Push)
├── sohara-processor/ # Data processing transforms
├── sohara-sink/      # Output sink implementations
├── sohara-js/        # QuickJS integration for scripting
└── sohara-cli/       # Command-line interface
```

## Status

🚧 **Early development** - This project is in its initial phase. The core abstractions are defined but many implementations are still TODO.

## License

MIT OR Apache-2.0
