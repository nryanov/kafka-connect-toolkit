# NormalizeFieldValue

*Transform · Common · [Overview](index.md)*

Change string value casing for selected fields in structs, arrays, and arrays of structs. For a plain string schema (no struct), use `:{FROM}:{TO}` with an empty field name.

Format: `{fieldName}:{from}:{to}`

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.NormalizeFieldValue$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.NormalizeFieldValue$Value` |

## Example

```properties
transforms=normalizeFieldValueInKey,normalizeFieldValueInValue

transforms.normalizeFieldValueInKey.type=com.nryanov.kafka.connect.toolkit.transforms.NormalizeFieldValue$Key
transforms.normalizeFieldValueInKey.fields=a:LOWER_HYPHEN:LOWER_UNDERSCORE,b:LOWER_CAMEL:UPPER_CAMEL,a.b.c:UPPER_UNDERSCORE:UPPER_CAMEL

transforms.normalizeFieldValueInValue.type=com.nryanov.kafka.connect.toolkit.transforms.NormalizeFieldValue$Value
transforms.normalizeFieldValueInValue.fields=a.b.c.d:UPPER_CAMEL:UPPER_UNDERSCORE
```

See [NormalizeFieldName](normalize-field-name.md) for allowed case format names.
