package com.nryanov.kafka.connect.toolkit.transforms;

import com.google.common.base.CaseFormat;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class NormalizeFieldValue<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
    sealed interface Mapper {
        String apply(String input);

        Mapper NONE_MAPPER = new NoneMapper();
    }

    private record NoneMapper() implements Mapper {
        public String apply(String input) {
            return input;
        }
    }

    private record FromToMapper(CaseFormat from, CaseFormat to) implements Mapper {
        public String apply(String input) {
            return from.to(to, input);
        }
    }

    private final static String FIELDS = "fields";
    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Comma separated list of fields in key part which should be transformed"
                    );

    private final Map<String, Mapper> fieldToCaseTypeMappings = new HashMap<>();

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);
        var fieldsRaw = config.getString(FIELDS);

        resolveMappings(fieldsRaw);
    }

    private void resolveMappings(String input) {
        Arrays.stream(input.split(","))
                .forEach(it -> {
                    var pair = it.split(":");

                    if (pair.length == 3) {
                        var fieldName = pair[0];
                        var from = CaseFormat.valueOf(pair[1]);
                        var to = CaseFormat.valueOf(pair[2]);

                        fieldToCaseTypeMappings.put(fieldName, new FromToMapper(from, to));
                    }
                });
    }

    protected Object applyMappings(Schema schema, Object input) {
        return applyMappings("", schema, input);
    }

    @SuppressWarnings("unchecked")
    private Object applyMappings(String fieldName, Schema schema, Object input) {
        return switch (schema.type()) {
            case STRUCT -> applyMappingsToStruct(fieldName, schema, (Struct) input);
            case ARRAY -> applyMappingsToArray(fieldName, schema, (List<Object>) input);
            case STRING -> applyMappingsToString(fieldName, schema, (String) input);
            case null, default -> input;
        };
    }

    private String applyMappingsToString(String fieldName, Schema schema, String input) {
        if (input == null) {
            return null;
        }

        return fieldToCaseTypeMappings.getOrDefault(fieldName, Mapper.NONE_MAPPER).apply(input);
    }

    private Struct applyMappingsToStruct(
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
            copiedStruct.put(field.name(), applyMappings(nextField, field.schema(), input.get(field)));
        }

        return copiedStruct;
    }

    private List<Object> applyMappingsToArray(String parentFieldName, Schema schema, List<Object> input) {
        if (input == null) {
            return null;
        }

        return input.stream().map(it -> applyMappings(parentFieldName, schema.valueSchema(), it)).toList();
    }

    public static class Key<R extends ConnectRecord<R>> extends NormalizeFieldValue<R> {
        @Override
        protected Object key(R record, Schema updatedSchema) {
            return applyMappings(record.keySchema(), record.key());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends NormalizeFieldValue<R> {
        @Override
        protected Object value(R record, Schema updatedSchema) {
            return applyMappings(record.valueSchema(), record.value());
        }
    }
}
