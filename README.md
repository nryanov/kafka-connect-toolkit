# kafka-connect-toolkit

## Toolkit
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
This transform allow to rename, exclude/include specified fields includes the nested ones. Also you don't need to set up different transforms for key or value because
this transform allows you to set up needed changes in a single config.
```properties
transforms=replaceFieldName
transforms.replaceFieldName.type=com.nryanov.kafka.connect.toolkit.ReplaceFieldName
# key
transforms.replaceFieldName.key.exclude=a,b.inner1.inner2
transforms.replaceFieldName.key.include=c,d.inner1
transforms.replaceFieldName.key.replace=c:renamed_c,d.inner1.inner2:renamed_inner_field
# value
transforms.replaceFieldName.value.exclude=a,b.inner1.inner2
transforms.replaceFieldName.value.include=c,d.inner1
transforms.replaceFieldName.value.replace=c:renamed_c,d.inner1.inner2:renamed_inner_field
```

As this transform's logic is similar to [confluent ReplaceField](https://docs.confluent.io/kafka-connectors/transforms/current/replacefield-confluent.html), you can find more examples in it's doc [confluent ReplaceField](https://docs.confluent.io/kafka-connectors/transforms/current/replacefield-confluent.html)

### ReplaceFieldValue
This transform allow to replace field values (including the nested ones).
Format of settings: `{field_name}:{replacement}`. If replacement couldn't be applied, then default value of type will be used.
```properties
transforms=replaceFieldValue
transforms.replaceFieldValue.type=com.nryanov.kafka.connect.toolkit.ReplaceFieldValue
# key
transforms.replaceFieldValue.key.fields=a:replacement
# value
transforms.replaceFieldName.value.fields=a.b.c:replacement
```

### NormalizeFieldValue
Change string value format of selected fields. This transform allow to change case of string values in nested fields (struct, arrays and arrays of structs).
If schema of key or value is just a plain string then use `:{FROM}:{TO}` (empty field name).

General format of configs is: `{fieldName}:{from}:{to}`
```properties
transforms=normalizeFieldValue
transforms.normalizeFieldValue.type=com.nryanov.kafka.connect.toolkit.NormalizeFieldValue
transforms.normalizeFieldValue.key.fields=a:LOWER_HYPHEN:LOWER_UNDERSCORE,b:LOWER_CAMEL:UPPER_CAMEL,a.b.c:UPPER_UNDERSCORE:UPPER_CAMEL
transforms.normalizeFieldValue.value.fields=a.b.c.d:UPPER_CAMEL:UPPER_UNDERSCORE
```

### NormalizeFieldName
Re-format schema field names to specified format  
```properties
transforms=normalizeFieldName
transforms.normalizeFieldName.type=com.nryanov.kafka.connect.toolkit.NormalizeFieldName
transforms.normalizeFieldName.case.from={LOWER_HYPHEN|LOWER_UNDERSCORE|LOWER_CAMEL|UPPER_CAMEL|UPPER_UNDERSCORE}
transforms.normalizeFieldName.case.to={LOWER_HYPHEN|LOWER_UNDERSCORE|LOWER_CAMEL|UPPER_CAMEL|UPPER_UNDERSCORE}
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