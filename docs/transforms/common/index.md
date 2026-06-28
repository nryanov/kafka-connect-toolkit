# Common transforms

*Transform · Common*

Connector-agnostic Kafka Connect Single Message Transforms from the `toolkit` module.

| | |
|---|---|
| **Artifact** | `toolkit.jar` |
| **Gradle module** | `modules/toolkit` |
| **Deploy to** | Kafka Connect `plugin.path` |

## Transforms

| Transform | Description |
|-----------|-------------|
| [DropSchemaless](drop-schemaless.md) | Drop record if key or value schema is null |
| [HeaderFromField](header-from-field.md) | Extract field value(s) into record headers |
| [CastToString](cast-to-string.md) | Cast numeric/boolean fields to string |
| [StringToHash](string-to-hash.md) | Replace string values with hex hash |
| [InsertHash](insert-hash.md) | Compute hash and insert as new field |
| [ConcatFields](concat-fields.md) | Concatenate fields into one string field |
| [SetNull](set-null.md) | Set key or value to null |
| [CopyFromTo](copy-from-to.md) | Copy fields between key and value |
| [SwapValueAndKey](swap-value-and-key.md) | Swap key and value |
| [BytesToBase64](bytes-to-base64.md) | Encode bytes as base64 string |
| [BytesToString](bytes-to-string.md) | Decode bytes to string |
| [DecimalAdjustScaleAndPrecision](decimal-adjust-scale-and-precision.md) | Adjust decimal precision and scale |
| [CardMask](card-mask.md) | Mask card numbers in text (Luhn) |
| [ReplaceFieldName](replace-field-name.md) | Rename, include, or exclude fields |
| [ReplaceFieldValue](replace-field-value.md) | Replace field values |
| [NormalizeFieldValue](normalize-field-value.md) | Normalize string value casing |
| [NormalizeFieldName](normalize-field-name.md) | Normalize schema field name casing |
