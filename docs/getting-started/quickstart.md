# Quickstart

## Common transform: DropSchemaless

Drop records when the key or value schema is null.

### 1. Build and deploy

```shell
make package
cp modules/toolkit/build/libs/toolkit.jar /path/to/kafka-connect/plugins/toolkit/
```

Restart Kafka Connect workers so the new plugin is loaded.

### 2. Configure the connector

Add the transform to your connector configuration:

```properties
transforms=dropSchemaless
transforms.dropSchemaless.type=com.nryanov.kafka.connect.toolkit.transforms.DropSchemaless$Key
```

Use `DropSchemaless$Value` for the value side. See [DropSchemaless](../transforms/common/drop-schemaless.md) for details.

### 3. Verify

Produce or consume records through the connector and confirm that schemaless records are dropped as expected.

---

## Debezium converter: TimestampConverter (Postgres)

Get predictable Avro logical types for Postgres temporal columns.

### 1. Build and deploy

```shell
make package
cp modules/debezium/debezium-postgres/build/libs/debezium-postgres.jar \
   /path/to/kafka-connect/plugins/debezium-connector-postgres/
```

### 2. Configure the connector

```properties
converters=timestampConverter
timestampConverter.type=com.nryanov.kafka.connect.toolkit.debezium.postgres.converters.TimestampConverter
timestampConverter.time.shift=-03:00
timestampConverter.timestamp.shift=-03:00
timestampConverter.timestamp.type=TIMESTAMP
timestampConverter.time.type=TIME
```

See [TimestampConverter](../converters/debezium/postgres/timestamp-converter.md) for the full property list and type mapping tables.

## Next steps

- [Install](install.md) — full deployment guide
- [Common transforms overview](../transforms/common/index.md)
