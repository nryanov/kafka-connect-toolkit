# kafka-connect-toolkit

## Toolkit
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