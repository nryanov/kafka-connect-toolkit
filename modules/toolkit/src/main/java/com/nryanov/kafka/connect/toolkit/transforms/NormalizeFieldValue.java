package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class NormalizeFieldValue<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String SELF = "__this__";
    enum CaseType {
        LOWER, UPPER
    }

    private final static String KEY_FIELDS = "key.fields";
    private final static String VALUE_FIELDS = "value.fields";
    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            KEY_FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Comma separated list of fields in key part which should be transformed"
                    )
                    .define(
                            VALUE_FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Comma separated list of fields in value part which should be transformed"
                    );

    private Map<String, CaseType> keyFieldToCaseTypeMappings;
    private Map<String, CaseType> valueFieldToCaseTypeMappings;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);
        var keyFields = Objects.requireNonNullElse(config.getString(KEY_FIELDS), "");
        var valueFields = Objects.requireNonNullElse(config.getString(VALUE_FIELDS), "");

        resolveMappings(keyFields, keyFieldToCaseTypeMappings);
        resolveMappings(valueFields, valueFieldToCaseTypeMappings);
    }

    private void resolveMappings(String input, Map<String, CaseType> map) {
        Arrays.stream(input.split(","))
                .forEach(it -> {
                    var pair = it.split(":");

                    var fieldName = pair[0];
                    var mapping = pair[1];

                    map.put(fieldName, CaseType.valueOf(mapping));
                });
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        var newKey = applyMappings(record.keySchema(), record.key());
        var newValue = applyMappings(record.valueSchema(), record.value());

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                newKey,
                record.valueSchema(),
                newValue,
                record.timestamp()
        );
    }

    @SuppressWarnings("unchecked")
    private Object applyMappings(Schema schema, Object input) {
        return switch (schema.type()) {
            case STRUCT -> applyMappingsToStruct(schema, (Struct) input);
            case ARRAY -> applyMappingsToArray(schema, (List<Object>) input);
            case STRING -> applyMappingsToString(schema, (String) input);
            case null, default -> input;
        };
    }

    private String applyMappingsToString(Schema schema, String input) {
        if (input == null) {
            return null;
        }

        // todo: for test only
        return input + "MAPPED";
    }

    private Struct applyMappingsToStruct(Schema schema, Struct input) {
        if (input == null) {
            return null;
        }

        var copiedStruct = new Struct(schema);

        for (var field : schema.fields()) {
            copiedStruct.put(field, applyMappings(field.schema(), input.get(field)));
        }

        return copiedStruct;
    }

    private List<Object> applyMappingsToArray(Schema schema, List<Object> input) {
        if (input == null) {
            return null;
        }

        return input.stream().map(it -> applyMappings(schema.valueSchema(), it)).toList();
    }
}
