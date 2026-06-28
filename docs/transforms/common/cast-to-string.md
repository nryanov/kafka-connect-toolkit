# CastToString

*Transform · Common · [Overview](index.md)*

Cast fields to string. Allowed input types: `FLOAT64`, `FLOAT32`, `BOOLEAN`, `INT8`, `INT16`, `INT32`, `INT64` and their array variants. Nested fields are supported.

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.CastToString$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.CastToString$Value` |

## Configuration

| Property | Description |
|----------|-------------|
| `fields` | Comma-separated field paths |

## Example

```properties
transforms=castToString
transforms.castToString.type=com.nryanov.kafka.connect.toolkit.transforms.CastToString$Key
transforms.castToString.fields=field,array.nested_field,struct.nested_level.nested_field
```
