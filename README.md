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
timestampConverter.timestamptz.type={STRING|TIMESTAMP} # default: TIMESTAMP
timestampConverter.date.type={STRING|DATE} # default: DATE
timestampConverter.time.type={STRING|TIME} # default: STRING
timestampConverter.timetz.type={STRING|TIME} # default: TIME
```

### SchemaRename
