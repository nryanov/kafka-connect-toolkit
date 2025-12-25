package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.nryanov.kafka.connect.toolkit.transforms.domain.common.ConfigParser.parseCommaSeparatedPairs;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ReplaceFieldValue<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
    private final static String NULL = "<NULL>";

    private final static String FIELDS = "fields";
    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Names of fields to replace in format {fieldName}:{replacement}"
                    );

    private sealed interface Replacement {
        Object value();

        NullReplacement nullReplacement = new NullReplacement();
    }

    private record NullReplacement() implements Replacement {
        @Override
        public Object value() {
            return null;
        }
    }

    // used if replacement value has another type and cannot be applied to the target field
    private record ZeroReplacement(Object replacement) implements Replacement {
        @Override
        public Object value() {
            return replacement;
        }
    }

    private record ExistingReplacement(Object replacement) implements Replacement {
        @Override
        public Object value() {
            return replacement;
        }
    }

    private Map<String, String> fieldReplacements;

    private final Map<String, Replacement> fieldMappedReplacements = new HashMap<>();

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);
        fieldReplacements = parseCommaSeparatedPairs(config, FIELDS);
    }

    protected Object applyReplacements(String parent, Schema schema, Object input) {
        if (input == null) {
            return null;
        }

        return switch (schema.type()) {
            case STRUCT -> applyReplacementsToStruct(parent, schema, input);
            case ARRAY -> applyReplacementsToArray(parent, schema, input);
            case STRING -> replaceString(parent, input);
            case BOOLEAN -> replaceBoolean(parent, input);
            case INT8 -> replaceByte(parent, input);
            case INT16 -> replaceShort(parent, input);
            case INT32 -> replaceInteger(parent, input);
            case INT64 -> replaceLong(parent, input);
            case FLOAT32 -> replaceFloat(parent, input);
            case FLOAT64 -> replaceDouble(parent, input);
            case null, default -> input;
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> applyReplacementsToArray(String parent, Schema schema, Object input) {
        var array = (List<Object>) input;

        return array.stream().map(it -> applyReplacements(parent, schema, it)).toList();
    }

    private Struct applyReplacementsToStruct(String parent, Schema schema, Object input) {
        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(schema);

        for (var field : schema.fields()) {
            var nextField = "".equals(parent) ? field.name() : parent + "." + field.name();
            newStruct.put(field.name(), applyReplacements(nextField, field.schema(), currentStruct.get(field)));
        }

        return newStruct;
    }

    private Object replaceString(String field, Object input) {
        return resolveMapping(field, input, it -> it, "");
    }

    private Object replaceBoolean(String field, Object input) {
        return resolveMapping(field, input, Boolean::parseBoolean, Boolean.FALSE);
    }

    private Object replaceByte(String field, Object input) {
        return resolveMapping(field, input, Byte::parseByte, (byte) 0);
    }

    private Object replaceShort(String field, Object input) {
        return resolveMapping(field, input, Short::parseShort, (short) 0);
    }

    private Object replaceInteger(String field, Object input) {
        return resolveMapping(field, input, Integer::parseInt, 0);
    }

    private Object replaceLong(String field, Object input) {
        return resolveMapping(field, input, Long::parseLong, (long) 0);
    }

    private Object replaceFloat(String field, Object input) {
        return resolveMapping(field, input, Float::parseFloat, (float) 0);
    }

    private Object replaceDouble(String field, Object input) {
        return resolveMapping(field, input, Double::parseDouble, (double) 0);
    }

    private Object resolveMapping(String field, Object input, Function<String, Object> mapping, Object defaultValue) {
        var replacement = fieldReplacements.get(field);
        if (replacement == null) {
            return input;
        }

        var mappedReplacement = fieldMappedReplacements
                .computeIfAbsent(field, ignored -> resolveReplacement(replacement, mapping, defaultValue));

        return mappedReplacement.value();
    }

    private Replacement resolveReplacement(String replacement, Function<String, Object> mapping, Object defaultValue) {
        if (NULL.equals(replacement)) {
            return Replacement.nullReplacement;
        }

        try {
            return new ExistingReplacement(mapping.apply(replacement));
        } catch (Exception e) {
            return new ZeroReplacement(defaultValue);
        }
    }

    public static class Key<R extends ConnectRecord<R>> extends ReplaceFieldValue<R> {
        @Override
        protected Object key(R record, Schema updatedSchema) {
            return applyReplacements("", record.keySchema(), record.key());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ReplaceFieldValue<R> {
        @Override
        protected Object value(R record, Schema updatedSchema) {
            return applyReplacements("", record.valueSchema(), record.value());
        }
    }
}
