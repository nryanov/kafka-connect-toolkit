# HeaderFromField

*Transform · Common · [Overview](index.md)*

Extract field value(s) and set them as record headers. Nested fields are supported.

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.HeaderFromField$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.HeaderFromField$Value` |

## Configuration

| Property | Description |
|----------|-------------|
| `mappings` | Comma-separated `field:header` pairs |

## Example

```properties
transforms=headerFromField
transforms.headerFromField.type=com.nryanov.kafka.connect.toolkit.transforms.HeaderFromField$Key
transforms.headerFromField.mappings=field:header1,nested.field.name:header2
```
