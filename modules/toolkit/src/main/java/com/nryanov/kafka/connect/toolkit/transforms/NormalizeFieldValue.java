package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class NormalizeFieldValue<R extends ConnectRecord<R>> implements Transformation<R> {
    enum CaseType {
        LOWER(String::toLowerCase),
        UPPER(String::toUpperCase),
        NONE(it -> it);

        private final Function<String, String> mapper;

        CaseType(Function<String, String> mapper) {
            this.mapper = mapper;
        }

        public String apply(String value) {
            return mapper.apply(value);
        }
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

    private final Map<String, CaseType> keyFieldToCaseTypeMappings = new HashMap<>();
    private final Map<String, CaseType> valueFieldToCaseTypeMappings = new HashMap<>();

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

                    if (pair.length == 2) {
                        var fieldName = pair[0];
                        var mapping = pair[1];

                        map.put(fieldName, CaseType.valueOf(mapping));
                    }
                });
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        var newKey = applyMappings(keyFieldToCaseTypeMappings, record.keySchema(), record.key());
        var newValue = applyMappings(valueFieldToCaseTypeMappings, record.valueSchema(), record.value());

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

    private Object applyMappings(
            Map<String, CaseType> mappings,
            Schema schema,
            Object input
    ) {
        return applyMappings(mappings,"", schema, input);
    }

    @SuppressWarnings("unchecked")
    private Object applyMappings(
            Map<String, CaseType> mappings,
            String fieldName,
            Schema schema,
            Object input
    ) {
        return switch (schema.type()) {
            case STRUCT -> applyMappingsToStruct(mappings, fieldName, schema, (Struct) input);
            case ARRAY -> applyMappingsToArray(mappings, fieldName, schema, (List<Object>) input);
            case STRING -> applyMappingsToString(mappings, fieldName, schema, (String) input);
            case null, default -> input;
        };
    }

    private String applyMappingsToString(
            Map<String, CaseType> mappings,
            String fieldName,
            Schema schema,
            String input
    ) {
        if (input == null) {
            return null;
        }

        return mappings.getOrDefault(fieldName, CaseType.NONE).apply(input);
    }

    private Struct applyMappingsToStruct(
            Map<String, CaseType> mappings,
            String parentFieldName,
            Schema schema,
            Struct input
    ) {
        if (input == null) {
            return null;
        }

        var copiedStruct = new Struct(schema);

        for (var field : schema.fields()) {
            var nextField = "".equals(parentFieldName) ? field.name() : parentFieldName + "." + field.name();
            copiedStruct.put(field, applyMappings(mappings, nextField, field.schema(), input.get(field)));
        }

        return copiedStruct;
    }

    private List<Object> applyMappingsToArray(
            Map<String, CaseType> mappings,
            String parentFieldName,
            Schema schema,
            List<Object> input
    ) {
        if (input == null) {
            return null;
        }

        return input.stream().map(it -> applyMappings(mappings, parentFieldName, schema.valueSchema(), it)).toList();
    }
}
