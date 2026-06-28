# ReplaceFieldName

*Transform · Common · [Overview](index.md)*

Rename, include, or exclude fields including nested ones.

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.ReplaceFieldName$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.ReplaceFieldName$Value` |

## Configuration

| Property | Description |
|----------|-------------|
| `exclude` | Comma-separated fields to remove |
| `include` | Comma-separated fields to keep |
| `replace` | Comma-separated `old:new` rename pairs |

## Example

```properties
transforms=replaceFieldNameKey,replaceFieldNameValue

transforms.replaceFieldNameKey.type=com.nryanov.kafka.connect.toolkit.transforms.ReplaceFieldName$Key
transforms.replaceFieldNameKey.exclude=a,b.inner1.inner2
transforms.replaceFieldNameKey.include=c,d.inner1
transforms.replaceFieldNameKey.replace=c:renamed_c,d.inner1.inner2:renamed_inner_field

transforms.replaceFieldNameValue.type=com.nryanov.kafka.connect.toolkit.transforms.ReplaceFieldName$Value
transforms.replaceFieldNameValue.exclude=a,b.inner1.inner2
transforms.replaceFieldNameValue.include=c,d.inner1
transforms.replaceFieldNameValue.replace=c:renamed_c,d.inner1.inner2:renamed_inner_field
```

## Notes

Logic is similar to [Confluent ReplaceField](https://docs.confluent.io/kafka-connectors/transforms/current/replacefield-confluent.html); see that documentation for additional examples.
