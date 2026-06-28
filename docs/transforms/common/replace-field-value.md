# ReplaceFieldValue

*Transform · Common · [Overview](index.md)*

Replace field values including nested fields. Format: `{field_name}:{replacement}`. If replacement cannot be applied, the type default is used.

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.ReplaceFieldValue$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.ReplaceFieldValue$Value` |

## Configuration

| Property | Description |
|----------|-------------|
| `fields` | Comma-separated `field:replacement` pairs |

## Example

```properties
transforms=replaceFieldValueInKey,replaceFieldValueInValue

transforms.replaceFieldValueInKey.type=com.nryanov.kafka.connect.toolkit.transforms.ReplaceFieldValue$Key
transforms.replaceFieldValueInKey.fields=a.b.c:replacement,d:replacement

transforms.replaceFieldValueInValue.type=com.nryanov.kafka.connect.toolkit.transforms.ReplaceFieldValue$Value
transforms.replaceFieldValueInValue.fields=a:replacement
```
