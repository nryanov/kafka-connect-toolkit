# TimestampConverter

*Converter · Debezium · Postgres · [Overview](index.md)*

Produce predictable results for PostgreSQL temporal types, especially in Avro schemas with proper logical types.

## Class

`com.nryanov.kafka.connect.toolkit.debezium.postgres.converters.TimestampConverter`

## Problem

Default Debezium Postgres temporal mapping (example with source TZ GMT+3):

| JDBC type | Raw input | Output | Output type (Avro) |
|:----------|:----------|:-------|:-------------------|
| TIMESTAMP WITHOUT TIME ZONE | 2025-01-01 12:00:00 | 1735732800000000 | `io.debezium.time.MicroTimestamp` |
| TIMESTAMP WITH TIME ZONE | 2025-01-01 12:00:00 | 2025-01-01T09:00:00.000000Z | `io.debezium.time.ZonedTimestamp` (string) |
| DATE | 2025-01-01 | 20089 | `io.debezium.time.Date` |
| TIME WITHOUT TIME ZONE | 12:00:00 | 43200000000 | `io.debezium.time.MicroTime` |
| TIME WITH TIME ZONE | 12:00:00 | 09:00:00Z | `io.debezium.time.ZonedTime` (string) |

Avro output often lacks standard logical types. Timezone-aware types may be strings while others are numeric, which affects downstream systems (Iceberg, Parquet, ORC) and loses temporal type metadata.

## Default converter behavior

With no extra configuration:

| JDBC type | Raw input | Output | Output type (Avro) |
|:----------|:----------|:-------|:-------------------|
| TIMESTAMP WITHOUT TIME ZONE | 2025-01-01 12:00:00 | 2025-01-01T12:00:00.000 | `["null","string"]` |
| TIMESTAMP WITH TIME ZONE | 2025-01-01 12:00:00 | 1735722000000 | `timestamp-millis` logical type |
| DATE | 2025-01-01 | 20089 | `date` logical type |
| TIME WITHOUT TIME ZONE | 12:00:00 | 12:00:00.000 | `["null","string"]` |
| TIME WITH TIME ZONE | 12:00:00 | 32400000 | `time-millis` logical type |

Date, timestamptz, and timetz get concrete logical types that Avro libraries handle natively. Timestamp and time without timezone are returned as formatted strings representing local wall-clock time (Postgres does not store the original timezone for those types).

## Timezone shift and type override

When you know timestamps and times were stored in a specific timezone (e.g. GMT+3):

```properties
converters=timestampConverter
timestampConverter.type=com.nryanov.kafka.connect.toolkit.debezium.postgres.converters.TimestampConverter
timestampConverter.time.shift=-03:00
timestampConverter.timestamp.shift=-03:00
timestampConverter.timestamp.type=TIMESTAMP
timestampConverter.time.type=TIME
```

This allows shifting timezone-less types (to UTC if needed) and choosing logical types instead of strings.

Result for timestamp and time without timezone:

| JDBC type | Raw input | Output | Output type (Avro) |
|:----------|:----------|:-------|:-------------------|
| TIMESTAMP WITHOUT TIME ZONE | 2025-01-01 12:00:00 | 1735722000000 | `timestamp-millis` logical type |
| TIME WITHOUT TIME ZONE | 12:00:00 | 32400000 | `time-millis` logical type |

Set every JDBC type output to `STRING` with a custom pattern if you prefer all string representations.

## Complete configuration

```properties
converters=timestampConverter
timestampConverter.type=com.nryanov.kafka.connect.toolkit.debezium.postgres.converters.TimestampConverter

# optional properties
## shift timestamp and time without timezone
timestampConverter.time.shift=+03:00
timestampConverter.timestamp.shift=+03:00

## timestamp without timezone
timestampConverter.timestamp.type=STRING
timestampConverter.timestamp.pattern=yyyy-MM-dd'T'HH:mm:ss.SSS

## timestamp with timezone
timestampConverter.timestamptz.type=TIMESTAMP
timestampConverter.timestamptz.pattern=yyyy-MM-dd'T'HH:mm:ss.SSS'Z'

## date
timestampConverter.date.type=DATE
timestampConverter.date.pattern=yyyy-MM-dd

## time without timezone
timestampConverter.time.type=STRING
timestampConverter.time.pattern=HH:mm:ss.SSS

## time with timezone
timestampConverter.timetz.type=TIME
timestampConverter.timetz.pattern=HH:mm:ss.SSS'Z'
```

### Property reference

| Property | Allowed values | Default |
|----------|------------------|---------|
| `time.shift` | ISO offset (e.g. `+03:00`) | `+00:00` |
| `timestamp.shift` | ISO offset | `+00:00` |
| `timestamp.type` | `STRING`, `TIMESTAMP` | `STRING` |
| `timestamp.pattern` | DateTimeFormatter pattern (when type=STRING) | `yyyy-MM-dd'T'HH:mm:ss.SSS` |
| `timestamptz.type` | `STRING`, `TIMESTAMP` | `TIMESTAMP` |
| `timestamptz.pattern` | Pattern (when type=STRING) | `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'` |
| `date.type` | `STRING`, `DATE` | `DATE` |
| `date.pattern` | Pattern (when type=STRING) | `yyyy-MM-dd` |
| `time.type` | `STRING`, `TIME` | `STRING` |
| `time.pattern` | Pattern (when type=STRING) | `HH:mm:ss.SSS` |
| `timetz.type` | `STRING`, `TIME` | `TIME` |
| `timetz.pattern` | Pattern (when type=STRING) | `HH:mm:ss.SSS'Z'` |
