# Install

## Prerequisites

- JDK 21
- `make` or Gradle wrapper (`./gradlew`)

## Build

```shell
make package
```

This runs `./gradlew jar` and produces shadow jars:

| Jar | Used by | Build output | Deploy target | Docs section |
|-----|---------|--------------|---------------|--------------|
| `toolkit.jar` | Common transforms | `modules/toolkit/build/libs/` | Kafka Connect `plugin.path` | [Transforms → Common](../transforms/common/index.md) |
| `debezium.jar` | Debezium-common transforms | `modules/debezium/build/libs/` | Kafka Connect `plugin.path` | [Transforms → Debezium → Common](../transforms/debezium/common/index.md) |
| `debezium-postgres.jar` | Postgres converters | `modules/debezium/debezium-postgres/build/libs/` | Debezium connector plugin directory | [Converters → Debezium → Postgres](../converters/debezium/postgres/index.md) |

## Deploy

Copy the built jars to each Kafka Connect worker node:

1. **Transforms** — place jars in a directory listed in `plugin.path`. See [Confluent custom transforms](https://docs.confluent.io/platform/current/connect/transforms/custom.html).
2. **Converters** — place `debezium-postgres.jar` in the Debezium connector plugin directory (alongside the connector jar). See [Debezium converters](https://debezium.io/documentation/reference/stable/development/converters.html).

Restart Connect workers (or reload the connector) after adding jars.

## Next steps

- [Quickstart](quickstart.md) — configure a transform or converter
- Browse [Transforms](../transforms/common/index.md) or [Converters](../converters/debezium/postgres/index.md)
