# kafka-connect-toolkit

## Toolkit

## Debezium

### TimestampConverter
```shell
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
```shell
transforms=schemaRename
transforms.schemaRename.type=com.nryanov.kafka.connect.toolkit.debezium.transforms.SchemaRename
transforms.schemaRename.internal.name={new_name} # if not set then transform will not change any records

# optional
transforms.schemaRename.cache.size={cache_size} # default: 32
```