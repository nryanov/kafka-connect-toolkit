# kafka-connect-toolkit

Custom [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) Single Message Transforms (SMTs) and [Debezium](https://debezium.io/) converters for common data-pipeline tasks: field manipulation, masking, hashing, schema normalization, and Postgres temporal type handling.

## What's included

Documentation is organized by component type:

- **[Transforms](transforms/common/index.md)** — record-level SMTs, split into **Common** (connector-agnostic) and **Debezium** (with DB-specific submodules)
- **[Converters](converters/debezium/postgres/index.md)** — Debezium value/key converters, grouped by database (Postgres, etc.)

## Requirements

- Java 21
- Kafka Connect runtime (Confluent Platform, Strimzi, etc.)

## Get started

1. [Install](getting-started/install.md) — build jars and deploy to Connect
2. [Quickstart](getting-started/quickstart.md) — configure your first transform or converter
