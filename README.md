# kafka-connect-toolkit
- Build & setup
  - [Build](#build)
  - [Setup](#setup)
- Toolkit
  - [DropSchemaless](#dropschemaless)
  - [HeaderFromField](#headerfromfield)
  - [CastToString](#casttostring)
  - [StringToHash](#stringtohash)
  - [InsertHash](#inserthash)
  - [ConcatFields](#concatfields)
  - [SetNull](#setnull)
  - [CopyFromTo](#copyfromto)
  - [SwapValueAndKey](#swapvalueandkey)
  - [BytesToBase64](#bytestobase64)
  - [BytesToString](#bytestostring)
  - [DecimalAdjustScaleAndPrecision](#decimaladjustscaleandprecision)
  - [CardMask](#cardmask)
  - [ReplaceFieldName](#replacefieldname)
  - [ReplaceFieldValue](#replacefieldvalue)
  - [NormalizeFieldValue](#normalizefieldvalue)
  - [NormalizeFieldName](#normalizefieldname)
- Debezium
  - [TimestampConverter](#timestampconverter-postgres)
  - [SchemaRename](#schemarename)

## Build & setup
### Build
To build jars use:
```shell
make package
```

Jars will be placed in:
- toolkit: `./modules/toolkit/build/libs`
- debezium: `./modules/debezium/build/libs`

### Setup
Built jars should be placed in each kafka-connect node.
- Custom **transforms** should be placed in a path, defined in `plugin.path` ([more info](https://docs.confluent.io/platform/current/connect/transforms/custom.html))
- Custom **converters** should be placed in each connector ([more info](https://debezium.io/documentation/reference/stable/development/converters.html))

## Toolkit
### DropSchemaless
Allow to drop record if key or value schema is null
- transform for key: `com.nryanov.kafka.connect.toolkit.transforms.DropSchemaless$Key`
- transform for value: `com.nryanov.kafka.connect.toolkit.transforms.DropSchemaless$Value`

```properties
transforms=dropSchemaless
transforms.dropSchemaless.type=com.nryanov.kafka.connect.toolkit.transforms.DropSchemaless$Key
```

### HeaderFromField
Extract field(s) value and set it as header. 
Nested fields are also allowed.

- transform for key: `com.nryanov.kafka.connect.toolkit.transforms.HeaderFromField$Key`
- transform for value: `com.nryanov.kafka.connect.toolkit.transforms.HeaderFromField$Value`

```properties
transforms=headerFromField
transforms.headerFromField.type=com.nryanov.kafka.connect.toolkit.transforms.HeaderFromField$Key
transforms.headerFromField.mappings=field:header2,nested.field.name:header2
```

### CastToString
Cast field to string. Allowed input field types: `FLOAT64, FLOAT32, BOOLEAN, INT8, INT16, INT32, INT64` and their array alternatives.
Nested fields are also allowed.

- transform for key: `com.nryanov.kafka.connect.toolkit.transforms.CastToString$Key`
- transform for value: `com.nryanov.kafka.connect.toolkit.transforms.CastToString$Value`

```properties
transforms=castToString
transforms.castToString.type=com.nryanov.kafka.connect.toolkit.transforms.CastToString$Key
transforms.castToString.fields=field,array.nested_field,struct.nested_level.nested_field
```

### StringToHash
Replace string values by it's calculated hash value in hex format. Allowed hash algorithms:
- MD5
- SHA1
- SHA256

Input field must be a type of string (or array of strings). Nested fields are also allowed.

- transform for key: `com.nryanov.kafka.connect.toolkit.transforms.InsertHash$Key`
- transform for value: `com.nryanov.kafka.connect.toolkit.transforms.InsertHash$Value`

```properties
transforms=stringToHash
transforms.stringToHash.type=com.nryanov.kafka.connect.toolkit.transforms.StringToHash$Key
transforms.stringToHash.fields=field:md5,array.nested_field:sha1,struct.nested_level.nested_field:sha256
```

### InsertHash
Calculate hash value from selected field and insert the result as a new field. Allowed algorithms:
- MD5
- SHA1
- SHA256

Input field must be a type of string.

- transform for key: `com.nryanov.kafka.connect.toolkit.transforms.InsertHash$Key`
- transform for value: `com.nryanov.kafka.connect.toolkit.transforms.InsertHash$Value`

```properties
transforms=insertHash
transforms.insertHash.type=com.nryanov.kafka.connect.toolkit.transforms.InsertHash$Key

transforms.insertHash.input.fields={input field. may be nested}
transforms.insertHash.output.field={output field name}
transforms.insertHash.algorithm={algorithm} # default: md5
```

### ConcatFields
Concat selected fields in the new single optional string field. Allow to concat nested fields (in arrays, structs). 
Only leaf fields are supported. If non-leaf field is selected, then it will be considered as NULL.

- transform for key: `com.nryanov.kafka.connect.toolkit.transforms.ConcatFields$Key`
- transform for value: `com.nryanov.kafka.connect.toolkit.transforms.ConcatFields$Value`

```properties
transforms=concatKeyFields
transforms.concatKeyFields.type=com.nryanov.kafka.connect.toolkit.transforms.ConcatFields$Key

transforms.concatKeyFields.input.fields={comma separated fields}
transforms.concatKeyFields.input.fields.null-replacement={replacement for null values} # default: ""
transforms.concatKeyFields.output.field={output field name}
transforms.concatKeyFields.delimiter={delimiter} # default _
```

### SetNull
Set value or key as null (payload & schema)

```properties
transforms=setNullKey,setNullValue
transforms.setNullKey.type=com.nryanov.kafka.connect.toolkit.transforms.SetNull$Key
transforms.setNullValue.type=com.nryanov.kafka.connect.toolkit.transforms.SetNull$Value
```

### CopyFromTo
Copies specified (or all) fields from source part to target part. Allows to specify:
- All fields `*`
- Concrete fields including nested in format: `{full_field_path:name}`

In result copied fields will be added to the root structure (not in the nested ones).
If field should be copied (e.g. leaf field of nested struct -> nested struct field should also be copied) but it has no name mapping then `field.name + suffix` will be used. `suffix` can be changed.

```properties
transforms=keyToValue,valueToKey
transforms.keyToValue.type=com.nryanov.kafka.connect.toolkit.transforms.CopyFrom$KeyToValue
transforms.keyToValue.fields=field1,field2,nested.inner
transforms.keyToValue.suffix=_key

transforms.valueToKey.type=com.nryanov.kafka.connect.toolkit.transforms.CopyFrom$ValueToKey
transforms.valueToKey.fields=*
```

### SwapValueAndKey
Swap key and value of record
```properties
transforms=swapValueAndKey
transforms.swapValueAndKey.type=com.nryanov.kafka.connect.toolkit.transforms.SwapValueAndKey
```

### BytesToBase64
Encode bytes into base64 string. This transform allows:
- Specify all fields using `*`
- Specify concrete fields to change. You can set only parent fields -- in this case all child fields also will be changed

```properties
transforms=bytesToBase64Key,bytesToBase64Value

transforms.bytesToBase64Key.type=com.nryanov.kafka.connect.toolkit.transforms.BytesToBase64$Key
transforms.bytesToBase64Key.fields=field,array.inner,struct.nested.inner

transforms.bytesToBase64Value.type=com.nryanov.kafka.connect.toolkit.transforms.BytesToBase64$Value
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

transforms.bytesToStringKey.type=com.nryanov.kafka.connect.toolkit.transforms.BytesToString$Key
transforms.bytesToStringKey.fields=field,array.inner,struct.nested.inner
transforms.bytesToStringKey.charset=WIN1251

transforms.bytesToStringValue.type=com.nryanov.kafka.connect.toolkit.transforms.BytesToString$Value
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

Transforms:
- Key: `com.nryanov.kafka.connect.toolkit.transforms.DecimalAdjustScaleAndPrecision$Key`
- Value: `com.nryanov.kafka.connect.toolkit.transforms.DecimalAdjustScaleAndPrecision$Value`

```properties
transforms=decimalAdjustScaleAndPrecision
transforms.decimalAdjustScaleAndPrecision.type=com.nryanov.kafka.connect.toolkit.transforms.DecimalAdjustScaleAndPrecision$Key

transforms.decimalAdjustScaleAndPrecision.fields={comma-separated list of fields in key-part | *}
transforms.decimalAdjustScaleAndPrecision.precision.value={target precision value} # default: null
transforms.decimalAdjustScaleAndPrecision.precision.mode={NONE|IF_NOT_SET|VALUE|LIMIT} # default: NONE
transforms.decimalAdjustScaleAndPrecision.precision.undefined-value={value which should be considered as undefined} # default: -1
transforms.decimalAdjustScaleAndPrecision.scale.value={target scale value} # default: null
transforms.decimalAdjustScaleAndPrecision.scale.mode={NONE|IF_NOT_SET|VALUE|LIMIT} # default: NONE
transforms.decimalAdjustScaleAndPrecision.scale.zero-mode={NONE|VALUE} # default: NONE
transforms.decimalAdjustScaleAndPrecision.scale.negative-mode={NONE|VALUE} # default: NONE
transforms.decimalAdjustScaleAndPrecision.scale.undefined-value={value which should be considered as undefined} # default: -1
```

### CardMask
`CardMask` transform allow to mask card number(s) in a text value. Under the hood a Luhn algorithm is used to determine a valid card numbers for masking.
To set up this transform in a minimum configuration you should set field(s) of which may contain card numbers which should be masked.
Nested fields are also supported.

Transforms:
- Key: `com.nryanov.kafka.connect.toolkit.transforms.CardMask$Key`
- Value: `com.nryanov.kafka.connect.toolkit.transforms.CardMask$Value`

```properties
transforms=cardMaskKey
transforms.cardMaskKey.type=com.nryanov.kafka.connect.toolkit.transforms.CardMask$Key

transforms.cardMaskKey.fields={comma-separated list of fields in key-part}
transforms.cardMaskKey.masking.expose-first-count={number of digits in the beginning which should be exposed in masked card number} # default: 4
transforms.cardMaskKey.masking.expose-last-count={number of digits in the end which should be exposed in masked card number} # default: 4
transforms.cardMaskKey.masking.character={character which should be used to mask digits} # default: *
transforms.cardMaskKey.masking.separators={characters which should be considered as valid separators of blocks in card-number} # default: - (+ space)
transforms.cardMaskKey.masking.card-number-lower-bound={minimum allowed length of card number} # default: 15
transforms.cardMaskKey.masking.card-number-upper-bound={maximum allowed length of card number # default: 16
```

### ReplaceFieldName
This transform allow to rename, exclude/include specified fields includes the nested ones. Supports nested fields.
```properties
transforms=replaceFieldNameKey,replaceFieldNameValue
# key
transforms.replaceFieldNameKey.type=com.nryanov.kafka.connect.toolkit.transforms.ReplaceFieldName$Key
transforms.replaceFieldNameKey.exclude=a,b.inner1.inner2
transforms.replaceFieldNameKey.include=c,d.inner1
transforms.replaceFieldNameKey.replace=c:renamed_c,d.inner1.inner2:renamed_inner_field
# value
transforms.replaceFieldNameValue.type=com.nryanov.kafka.connect.toolkit.transforms.ReplaceFieldName$Value
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
transforms.replaceFieldValueInKey.type=com.nryanov.kafka.connect.toolkit.transforms.ReplaceFieldValue$Key
transforms.replaceFieldValueInKey.fields=a.b.c:replacement,d:replacement

transforms.replaceFieldValueInValue.type=com.nryanov.kafka.connect.toolkit.transforms.ReplaceFieldValue$Value
transforms.replaceFieldValueInValue.fields=a:replacement
```

### NormalizeFieldValue
Change string value format of selected fields. This transform allow to change case of string values in nested fields (struct, arrays and arrays of structs).
If schema of key or value is just a plain string then use `:{FROM}:{TO}` (empty field name).

General format of configs is: `{fieldName}:{from}:{to}`
```properties
transforms=normalizeFieldValueInKey,normalizeFieldValueInValue

transforms.normalizeFieldValueInKey.type=com.nryanov.kafka.connect.toolkit.transforms.NormalizeFieldValue$Key
transforms.normalizeFieldValueInKey.fields=a:LOWER_HYPHEN:LOWER_UNDERSCORE,b:LOWER_CAMEL:UPPER_CAMEL,a.b.c:UPPER_UNDERSCORE:UPPER_CAMEL

transforms.normalizeFieldValueInValue.type=com.nryanov.kafka.connect.toolkit.transforms.NormalizeFieldValue$Value
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

transforms.normalizeFieldNameKey.type=com.nryanov.kafka.connect.toolkit.transforms.NormalizeFieldName$Key
transforms.normalizeFieldNameKey.case.from=LOWER_HYPHEN
transforms.normalizeFieldNameKey.case.to=LOWER_UNDERSCORE

transforms.normalizeFieldNameValue.type=com.nryanov.kafka.connect.toolkit.transforms.NormalizeFieldName$Value
transforms.normalizeFieldNameValue.case.from=UPPER_UNDERSCORE
transforms.normalizeFieldNameValue.case.to=UPPER_CAMEL
```

## Debezium

### TimestampConverter (Postgres)
The goal of this converter is to get predictable results from DB for temporal types especially for avro schemas.
For example, initially these types will be generated for the next fields (for types with timezone initial TZ was GMT+3):

| jdbc type                   | raw input           | output                      | output type (avro)                                                                     |
|:----------------------------|:--------------------|:----------------------------|:---------------------------------------------------------------------------------------|
| TIMESTAMP WITHOUT TIME ZONE | 2025-01-01 12:00:00 | 1735732800000000            | {"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTimestamp"}   |
| TIMESTAMP WITH TIME ZONE    | 2025-01-01 12:00:00 | 2025-01-01T09:00:00.000000Z | {"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTimestamp"} |
| DATE                        | 2025-01-01          | 20089                       | {"type":"int","connect.version":1,"connect.name":"io.debezium.time.Date"}              |
| TIME WITHOUT TIME ZONE      | 12:00:00            | 43200000000                 | {"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTime"}        |
| TIME WITH TIME ZONE         | 12:00:00            | 09:00:00Z                   | {"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTime"}      |

As you can see, for avro there were no any logicalTypes. Also types **with timezone** returned as strings (which may be ok for some cases) while other types returned as some numerics.
In the end it will affect how these values will be saved in the target system (like iceberg table, parquet/orc file, etc) and these fields will lost any info about its initial type (like that it was a temporal one).

To overcome it, one possible solution is to use custom converter. By default, without any additional configs, it will generate the next results for the same types:

| jdbc type                   | raw input           | output                  | output type (avro)                                                                                                                     |
|:----------------------------|:--------------------|:------------------------|:---------------------------------------------------------------------------------------------------------------------------------------|
| TIMESTAMP WITHOUT TIME ZONE | 2025-01-01 12:00:00 | 2025-01-01T12:00:00.000 | ["null","string"]                                                                                                                      |
| TIMESTAMP WITH TIME ZONE    | 2025-01-01 12:00:00 | 1735722000000           | ["null",{"type":"long","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Timestamp","logicalType":"timestamp-millis"}] |
| DATE                        | 2025-01-01          | 20089                   | ["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Date","logicalType":"date"}]                   |
| TIME WITHOUT TIME ZONE      | 12:00:00            | 12:00:00.000            | ["null","string"]                                                                                                                      |
| TIME WITH TIME ZONE         | 12:00:00            | 32400000                | ["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Time","logicalType":"time-millis"}]            |

Using this converter without any additional settings, date, timestamptz and timetz will have concrete logicalType, which any avro library can handle.
For other types plain string will be returned in some predefined format. Keep in mind, that this string representation of time **is not in UTC**, but it should be considered as **in some point in time in some local TZ**.
The reason for it is that Postgres does not save any information about TZ which was used during save for types without timezone.

But if you know exactly, that, e.g. timestamp and time were saved in GMT+3, then you can convert even these types to some logical counterpart:
```properties
converters=timestampConverter
timestampConverter.type=com.nryanov.kafka.connect.toolkit.debezium.postgres.converters.TimestampConverter
timestampConverter.time.shift=-03:00
timestampConverter.timestamp.shift=-03:00
timestampConverter.timestamp.type=TIMESTAMP
timestampConverter.time.type=TIME
```

Using this configuration, you can:
- Shift temporal types without timezone (and achieve UTC if needed)
- Choose final type which should be used instead of string

Finally, **timestamp** and **time** will be in like these:

| jdbc type                   | raw input           | output        | output type (avro)                                                                                                                     |
|:----------------------------|:--------------------|:--------------|:---------------------------------------------------------------------------------------------------------------------------------------|
| TIMESTAMP WITHOUT TIME ZONE | 2025-01-01 12:00:00 | 1735722000000 | ["null",{"type":"long","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Timestamp","logicalType":"timestamp-millis"}] |
| TIME WITHOUT TIME ZONE      | 12:00:00            | 32400000      | ["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Time","logicalType":"time-millis"}]            |

If you want to return everything just like strings, you can set up for every jdbc-type final output as `STRING` and also configure pattern.

Complete list of properties:
```properties
converters=timestampConverter
timestampConverter.type=com.nryanov.kafka.connect.toolkit.debezium.converters.TimestampConverter

# optional properties
## allows to shift timestamp and time 
timestampConverter.time.shift=+03:00 # default: +00:00
timestampConverter.timestamp.shift=+03:00 # default: +00:00

## allows to choose output type for timestamp without timezone
timestampConverter.timestamp.type={STRING|TIMESTAMP} # default: STRING
timestampConverter.timestamp.pattern={PATTERN -- when type=STRING} # default: yyyy-MM-dd'T'HH:mm:ss.SSS

## allows to choose output type for timestamp with timezone
timestampConverter.timestamptz.type={STRING|TIMESTAMP} # default: TIMESTAMP
timestampConverter.timestamptz.pattern={PATTERN -- when type=STRING} # default: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'

## allows to choose output type for date
timestampConverter.date.type={STRING|DATE} # default: DATE
timestampConverter.date.pattern={PATTERN -- when type=STRING} # default: yyyy-MM-dd"

## allows to choose output type for time without timezone
timestampConverter.time.type={STRING|TIME} # default: STRING
timestampConverter.time.pattern={PATTERN -- when type=STRING} # default: HH:mm:ss.SSS

## allows to choose output type for time with timezone
timestampConverter.timetz.type={STRING|TIME} # default: TIME
timestampConverter.timetz.pattern={PATTERN -- when type=STRING} # default: HH:mm:ss.SSS'Z'
```

### SchemaRename
For sharding and/or hypertable (timescaledb) debezium will generate different names for before/after schema even for the same tables (e.g. in different shards). 
Different names will affect how new schemas will be saved in Schema Registry (SR) if compatibility type != **NONE**. 
To overcome it, special transform should be used to re-name internal schemas:

```properties
transforms=schemaRename
transforms.schemaRename.type=com.nryanov.kafka.connect.toolkit.debezium.transforms.SchemaRename
transforms.schemaRename.internal.name={new_name} # if not set then transform will not change any records
```

In general, this transform should be used in pair with the default one [SetSchemaMetadata](https://docs.confluent.io/kafka-connectors/transforms/current/setschemametadata.html). In this case default transform will re-name outer schema, and this transform will re-name internal ones.
It will allow you to control schema evolution and avoid errors during schemas update in source DB.