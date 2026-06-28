# ConcatFields

*Transform · Common · [Overview](index.md)*

Concatenate selected fields into a new optional string field. Supports nested fields in arrays and structs. Only leaf fields are supported; non-leaf fields are treated as NULL.

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.ConcatFields$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.ConcatFields$Value` |

## Configuration

| Property | Description |
|----------|-------------|
| `input.fields` | Comma-separated source fields |
| `input.fields.null-replacement` | Replacement for null values (default: `""`) |
| `output.field` | Name of the output field |
| `delimiter` | Field delimiter (default: `_`) |

## Example

```properties
transforms=concatKeyFields
transforms.concatKeyFields.type=com.nryanov.kafka.connect.toolkit.transforms.ConcatFields$Key
transforms.concatKeyFields.input.fields=first_name,last_name
transforms.concatKeyFields.output.field=full_name
transforms.concatKeyFields.delimiter=_
```
