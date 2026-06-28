# Debezium common transforms

*Transform · Debezium · Common*

Debezium-specific transforms that work across database connectors.

| | |
|---|---|
| **Artifact** | `debezium.jar` |
| **Gradle module** | `modules/debezium` |
| **Deploy to** | Kafka Connect `plugin.path` |

## Transforms

| Transform | Description |
|-----------|-------------|
| [SchemaRename](schema-rename.md) | Rename internal Debezium envelope schemas for Schema Registry compatibility |

## Database-specific transforms

Postgres and other DB-specific Debezium transforms will appear under **Transforms → Debezium → {Database}** when added.
