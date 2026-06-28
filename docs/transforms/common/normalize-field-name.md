# NormalizeFieldName

*Transform · Common · [Overview](index.md)*

Reformat schema field names to a target casing convention.

Allowed formats:

- `LOWER_HYPHEN`
- `LOWER_UNDERSCORE`
- `LOWER_CAMEL`
- `UPPER_CAMEL`
- `UPPER_UNDERSCORE`

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.NormalizeFieldName$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.NormalizeFieldName$Value` |

## Configuration

| Property | Description |
|----------|-------------|
| `case.from` | Source naming convention |
| `case.to` | Target naming convention |

## Example

```properties
transforms=normalizeFieldNameKey,normalizeFieldNameValue

transforms.normalizeFieldNameKey.type=com.nryanov.kafka.connect.toolkit.transforms.NormalizeFieldName$Key
transforms.normalizeFieldNameKey.case.from=LOWER_HYPHEN
transforms.normalizeFieldNameKey.case.to=LOWER_UNDERSCORE

transforms.normalizeFieldNameValue.type=com.nryanov.kafka.connect.toolkit.transforms.NormalizeFieldName$Value
transforms.normalizeFieldNameValue.case.from=UPPER_UNDERSCORE
transforms.normalizeFieldNameValue.case.to=UPPER_CAMEL
```
