# SchemaRename

*Transform · Debezium · Common · [Overview](index.md)*

Rename internal Debezium envelope schemas. Useful for sharding and TimescaleDB hypertables where Debezium generates different before/after schema names for the same logical table across shards.

Different internal schema names can break Schema Registry compatibility when compatibility is not `NONE`.

## Classes

| Part | Class |
|------|-------|
| Both | `com.nryanov.kafka.connect.toolkit.debezium.transforms.SchemaRename` |

## Configuration

| Property | Description |
|----------|-------------|
| `internal.name` | New internal schema name; if unset, records are unchanged |

## Example

```properties
transforms=schemaRename
transforms.schemaRename.type=com.nryanov.kafka.connect.toolkit.debezium.transforms.SchemaRename
transforms.schemaRename.internal.name=my_unified_schema
```

## Notes

Use together with [Confluent SetSchemaMetadata](https://docs.confluent.io/kafka-connectors/transforms/current/setschemametadata.html): SetSchemaMetadata renames the outer schema; SchemaRename renames internal ones. This helps control schema evolution when the source database schema changes.
