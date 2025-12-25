# kafka-connect-toolkit
- Toolkit
  - [StringToHash](#stringtohash)
  - [InsertHash](#inserthash)
  - [ConcatFields](#concatfields)
  - [SetNull](#setnull)
  - [CopyFromTo](#copyfromto)
  - [SwapValueAndKey](#swapvalueandkey)
  - [BytesToBase64](#bytestobase64)
  - [BytesToString](#bytestostring)
  - [DecimalAdjustScaleAndPrecision](#decimaladjustscaleandprecision)
  - [CardMaskFieldValue](#cardmaskfieldvalue)
  - [ReplaceFieldName](#replacefieldname)
  - [ReplaceFieldValue](#replacefieldvalue)
  - [NormalizeFieldValue](#normalizefieldvalue)
  - [NormalizeFieldName](#normalizefieldname)
- Debezium
  - [TimestampConverter](#timestampconverter)
  - [SchemaRename](#schemarename)

## Toolkit
### StringToHash
Replace string values by it's calculated hash value in hex format. Allowed hash algorithms:
- MD5
- SHA1
- SHA256

Input field must be a type of string (or array of strings). Nested fields are also allowed.

- transform for key: `com.nryanov.kafka.connect.toolkit.InsertHash$Key`
- transform for value: `com.nryanov.kafka.connect.toolkit.InsertHash$Value`

```properties
transforms=stringToHash
transforms.stringToHash.type=com.nryanov.kafka.connect.toolkit.StringToHash$Key
transforms.stringToHash.fields=field:md5,array.nested_field:sha1,struct.nested_level.nested_field:sha256
```

### InsertHash
Calculate hash value from selected field and insert the result as a new field. Allowed algorithms:
- MD5
- SHA1
- SHA256

Input field must be a type of string.

- transform for key: `com.nryanov.kafka.connect.toolkit.InsertHash$Key`
- transform for value: `com.nryanov.kafka.connect.toolkit.InsertHash$Value`

```properties
transforms=insertHash
transforms.insertHash.type=com.nryanov.kafka.connect.toolkit.InsertHash$Key

transforms.insertHash.input.fields={input field. may be nested}
transforms.insertHash.output.field={output field name}
transforms.insertHash.algorithm={algorithm} # default: md5
```

### ConcatFields
Concat selected fields in the new single optional string field. Allow to concat nested fields (in arrays, structs). 
Only leaf fields are supported. If non-leaf field is selected, then it will be considered as NULL.

- transform for key: `com.nryanov.kafka.connect.toolkit.ConcatFields$Key`
- transform for value: `com.nryanov.kafka.connect.toolkit.ConcatFields$Value`

```properties
transforms=concatKeyFields
transforms.concatKeyFields.type=com.nryanov.kafka.connect.toolkit.ConcatFields$Key

transforms.concatKeyFields.input.fields={comma separated fields}
transforms.concatKeyFields.input.fields.null-replacement={replacement for null values} # default: ""
transforms.concatKeyFields.output.field={output field name}
transforms.concatKeyFields.delimiter={delimiter} # default _
```

### SetNull
Set value or key as null (payload & schema)

```properties
transforms=setNullKey,setNullValue
transforms.setNullKey.type=com.nryanov.kafka.connect.toolkit.SetNull$Key
transforms.setNullValue.type=com.nryanov.kafka.connect.toolkit.SetNull$Value
```

### CopyFromTo
Copies specified (or all) fields from source part to target part. Allows to specify:
- All fields `*`
- Concrete fields including nested in format: `{full_field_path:name}`

In result copied fields will be added to the root structure (not in the nested ones).
If field should be copied (e.g. leaf field of nested struct -> nested struct field should also be copied) but it has no name mapping then `field.name + suffix` will be used. `suffix` can be changed.

```properties
transforms=keyToValue,valueToKey
transforms.keyToValue.type=com.nryanov.kafka.connect.toolkit.CopyFrom$KeyToValue
transforms.keyToValue.fields=field1,field2,nested.inner
transforms.keyToValue.suffix=_key

transforms.valueToKey.type=com.nryanov.kafka.connect.toolkit.CopyFrom$ValueToKey
transforms.valueToKey.fields=*
```

### SwapValueAndKey
Swap key and value of record
```properties
transforms=swapValueAndKey
transforms.swapValueAndKey.type=com.nryanov.kafka.connect.toolkit.SwapValueAndKey
```

### BytesToBase64
Encode bytes into base64 string. This transform allows:
- Specify all fields using `*`
- Specify concrete fields to change. You can set only parent fields -- in this case all child fields also will be changed

```properties
transforms=bytesToBase64Key,bytesToBase64Value

transforms.bytesToBase64Key.type=com.nryanov.kafka.connect.toolkit.BytesToBase64$Key
transforms.bytesToBase64Key.fields=field,array.inner,struct.nested.inner

transforms.bytesToBase64Value.type=com.nryanov.kafka.connect.toolkit.BytesToBase64$Value
transforms.bytesToBase64Value.fields=*
```

### BytesToString
Decode bytes into string.
This transform allows:
- Specify all fields (in key,value part) using `*`
- Specify concrete fields to change. You can set only parent fields -- in this case all child fields also will be changed
- Specify charset which should be used to decode string

```properties
transforms=bytesToStringKey,bytesToStringValue

transforms.bytesToStringKey.type=com.nryanov.kafka.connect.toolkit.BytesToString$Key
transforms.bytesToStringKey.fields=field,array.inner,struct.nested.inner
transforms.bytesToStringKey.charset=WIN1251

transforms.bytesToStringValue.type=com.nryanov.kafka.connect.toolkit.BytesToString$Value
transforms.bytesToStringValue.fields=*
```

### DecimalAdjustScaleAndPrecision
Some target sinks may not support decimals with high precision, or you want to change precision/scale of decimals in event to desired values.
`DecimalAdjustScaleAndPrecision` transform allows to achieve it.
This transform allows:
- Specify all fields (in key,value part) using `*` 
- Specify concrete fields to change. You can set only parent fields -- in this case all child fields also will be changed
- Control whether to update precision using `precision.mode`. Allowed values:
  - NONE -- does not apply any updates (DEFAULT)
  - IF_NOT_SET -- set precision only if current precision is undefined
  - VALUE -- unconditionally set desired precision
  - LIMIT -- just truncate value's precision if it's current value is bigger than desired 
- Control whether to update scale using `scale.mode`. Allowed values:
    - NONE -- does not apply any updates (DEFAULT)
    - IF_NOT_SET -- set scale only if current scale is undefined
    - VALUE -- unconditionally set desired scale
    - LIMIT -- just truncate value's scale if it's current value is bigger than desired
- Specify value which should be considered as undefined for precision using `precision.undefined-value`. By default, it's `-1`
- Specify value which should be considered as undefined for scale using `scale.undefined-value`. By default, it's `-1`
- Update scale only if current scale is undefined using `scale.zero-mode`:
    - NONE -- does not apply any updates (DEFAULT)
    - VALUE -- set desired value for scale if current scale is zero
- Update scale only if current scale is negative using `scale.negative-mode`:
    - NONE -- does not apply any updates (DEFAULT)
    - VALUE -- set desired value for scale if current scale is negative

```properties
transforms=decimalAdjustScaleAndPrecision
transforms.decimalAdjustScaleAndPrecision.type=com.nryanov.kafka.connect.toolkit.DecimalAdjustScaleAndPrecision

transforms.decimalAdjustScaleAndPrecision.key.fields={comma-separated list of fields in key-part | *} # default: null
transforms.decimalAdjustScaleAndPrecision.value.fields={comma-separated list of fields in value-part | *} # default: null
transforms.decimalAdjustScaleAndPrecision.precision.value={target precision value} # default: null
transforms.decimalAdjustScaleAndPrecision.precision.mode={NONE|IF_NOT_SET|VALUE|LIMIT} # default: NONE
transforms.decimalAdjustScaleAndPrecision.precision.undefined-value={value which should be considered as undefined} # default: -1
transforms.decimalAdjustScaleAndPrecision.scale.value={target scale value} # default: null
transforms.decimalAdjustScaleAndPrecision.scale.mode={NONE|IF_NOT_SET|VALUE|LIMIT} # default: NONE
transforms.decimalAdjustScaleAndPrecision.scale.zero-mode={NONE|VALUE} # default: NONE
transforms.decimalAdjustScaleAndPrecision.scale.negative-mode={NONE|VALUE} # default: NONE
transforms.decimalAdjustScaleAndPrecision.scale.undefined-value={value which should be considered as undefined} # default: -1
```

### CardMaskFieldValue
`CardMaskFieldValue` transform allow to mask card number(s) in a text value. Under the hood a Luhn algorithm is used to determine a valid card numbers for masking.
To setup this transform in a minimum configuration you should set field(s) of key and/or value parts which may contain card numbers which should be masked.
Nested fields are also supported.

```properties
transforms=cardMaskFieldValue
transforms.cardMaskFieldValue.type=com.nryanov.kafka.connect.toolkit.CardMaskFieldValue

transforms.cardMaskFieldValue.key.fields={comma-separated list of fields in key-part}
transforms.cardMaskFieldValue.value.fields={comma-separated list of fields in value-part}
transforms.cardMaskFieldValue.masking.expose-first-count={number of digits in the beginning which should be exposed in masked card number} # default: 4
transforms.cardMaskFieldValue.masking.expose-last-count={number of digits in the end which should be exposed in masked card number} # default: 4
transforms.cardMaskFieldValue.masking.character={character which should be used to mask digits} # default: *
transforms.cardMaskFieldValue.masking.separators={characters which should be considered as valid separators of blocks in card-number} # default: - (+ space)
transforms.cardMaskFieldValue.masking.card-number-lower-bound={minimum allowed length of card number} # default: 15
transforms.cardMaskFieldValue.masking.card-number-upper-bound={maximum allowed length of card number # default: 16
```

### ReplaceFieldName
This transform allow to rename, exclude/include specified fields includes the nested ones. Supports nested fields.
```properties
transforms=replaceFieldNameKey,replaceFieldNameValue
# key
transforms.replaceFieldNameKey.type=com.nryanov.kafka.connect.toolkit.ReplaceFieldName$Key
transforms.replaceFieldNameKey.exclude=a,b.inner1.inner2
transforms.replaceFieldNameKey.include=c,d.inner1
transforms.replaceFieldNameKey.replace=c:renamed_c,d.inner1.inner2:renamed_inner_field
# value
transforms.replaceFieldNameValue.type=com.nryanov.kafka.connect.toolkit.ReplaceFieldName$Value
transforms.replaceFieldNameValue.exclude=a,b.inner1.inner2
transforms.replaceFieldNameValue.include=c,d.inner1
transforms.replaceFieldNameValue.replace=c:renamed_c,d.inner1.inner2:renamed_inner_field
```

As this transform's logic is similar to [confluent ReplaceField](https://docs.confluent.io/kafka-connectors/transforms/current/replacefield-confluent.html), you can find more examples in it's doc [confluent ReplaceField](https://docs.confluent.io/kafka-connectors/transforms/current/replacefield-confluent.html)

### ReplaceFieldValue
This transform allow to replace field values (including the nested ones).
Format of settings: `{field_name}:{replacement}`. If replacement couldn't be applied, then default value of type will be used.
```properties
transforms=replaceFieldValueInKey,replaceFieldValueInValue
transforms.replaceFieldValueInKey.type=com.nryanov.kafka.connect.toolkit.ReplaceFieldValue$Key
transforms.replaceFieldValueInKey.fields=a.b.c:replacement,d:replacement

transforms.replaceFieldValueInValue.type=com.nryanov.kafka.connect.toolkit.ReplaceFieldValue$Value
transforms.replaceFieldValueInValue.fields=a:replacement
```

### NormalizeFieldValue
Change string value format of selected fields. This transform allow to change case of string values in nested fields (struct, arrays and arrays of structs).
If schema of key or value is just a plain string then use `:{FROM}:{TO}` (empty field name).

General format of configs is: `{fieldName}:{from}:{to}`
```properties
transforms=normalizeFieldValueInKey,normalizeFieldValueInValue

transforms.normalizeFieldValueInKey.type=com.nryanov.kafka.connect.toolkit.NormalizeFieldValue$Key
transforms.normalizeFieldValueInKey.fields=a:LOWER_HYPHEN:LOWER_UNDERSCORE,b:LOWER_CAMEL:UPPER_CAMEL,a.b.c:UPPER_UNDERSCORE:UPPER_CAMEL

transforms.normalizeFieldValueInValue.type=com.nryanov.kafka.connect.toolkit.NormalizeFieldValue$Value
transforms.normalizeFieldValueInValue.fields=a.b.c.d:UPPER_CAMEL:UPPER_UNDERSCORE
```

### NormalizeFieldName
Re-format schema field names to specified format. Allowed values:
- LOWER_HYPHEN
- LOWER_UNDERSCORE
- LOWER_CAMEL
- UPPER_CAMEL
- UPPER_UNDERSCORE

```properties
transforms=normalizeFieldNameKey,normalizeFieldNameValue

transforms.normalizeFieldNameKey.type=com.nryanov.kafka.connect.toolkit.NormalizeFieldName$Key
transforms.normalizeFieldNameKey.case.from=LOWER_HYPHEN
transforms.normalizeFieldNameKey.case.to=LOWER_UNDERSCORE

transforms.normalizeFieldNameValue.type=com.nryanov.kafka.connect.toolkit.NormalizeFieldName$Value
transforms.normalizeFieldNameValue.case.from=UPPER_UNDERSCORE
transforms.normalizeFieldNameValue.case.to=UPPER_CAMEL
```

## Debezium

### TimestampConverter
```properties
converters=timestampConverter
timestampConverter.type=com.nryanov.kafka.connect.toolkit.debezium.converters.TimestampConverter

# optional properties

timestampConverter.time.shift=+03:00 # default: UTC (+0)
timestampConverter.timestamp.shift=+03:00 # default: UTC (+0)

timestampConverter.timestamp.type={STRING|TIMESTAMP} # default: STRING
timestampConverter.timestamp.pattern={PATTERN -- when type=STRING} # default: yyyy-MM-dd'T'HH:mm:ss.SSS

timestampConverter.timestamptz.type={STRING|TIMESTAMP} # default: TIMESTAMP
timestampConverter.timestamptz.pattern={PATTERN -- when type=STRING} # default: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'

timestampConverter.date.type={STRING|DATE} # default: DATE
timestampConverter.date.pattern={PATTERN -- when type=STRING} # default: yyyy-MM-dd"

timestampConverter.time.type={STRING|TIME} # default: STRING
timestampConverter.time.pattern={PATTERN -- when type=STRING} # default: HH:mm:ss.SSS

timestampConverter.timetz.type={STRING|TIME} # default: TIME
timestampConverter.timetz.pattern={PATTERN -- when type=STRING} # default: HH:mm:ss.SSS'Z'
```

### SchemaRename
```properties
transforms=schemaRename
transforms.schemaRename.type=com.nryanov.kafka.connect.toolkit.debezium.transforms.SchemaRename
transforms.schemaRename.internal.name={new_name} # if not set then transform will not change any records

# optional
transforms.schemaRename.cache.size={cache_size} # default: 32
```