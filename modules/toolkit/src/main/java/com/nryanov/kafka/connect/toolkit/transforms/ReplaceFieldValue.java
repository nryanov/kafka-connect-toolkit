package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.domain.model.Target;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.nryanov.kafka.connect.toolkit.transforms.domain.common.ConfigParser.parseCommaSeparatedPairs;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class ReplaceFieldValue<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String NULL = "<NULL>";

    private final static String KEY_REPLACE = "key.fields";
    private final static String VALUE_REPLACE = "value.fields";
    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            KEY_REPLACE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Names of fields to replace in key part"
                    )
                    .define(
                            VALUE_REPLACE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Names of fields to replace in value part"
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

    private Map<String, String> keyFieldReplacements;
    private Map<String, String> valueFieldReplacements;

    private final Map<String, Replacement> keyFieldMappedReplacements = new HashMap<>();
    private final Map<String, Replacement> valueFieldMappedReplacements = new HashMap<>();

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);

        keyFieldReplacements = parseCommaSeparatedPairs(config, KEY_REPLACE);
        valueFieldReplacements = parseCommaSeparatedPairs(config, VALUE_REPLACE);
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        var initialParentPath = "";

        var mappedKey = record.key() == null ? null : applyReplacements(Target.KEY, initialParentPath, record.keySchema(), record.key());
        var mappedValue = record.value() == null ? null : applyReplacements(Target.VALUE, initialParentPath, record.valueSchema(), record.value());

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                mappedKey,
                record.valueSchema(),
                mappedValue,
                record.timestamp()
        );
    }

    private Object applyReplacements(Target target, String parent, Schema schema, Object input) {
        if (schema == null) {
            return null;
        }

        return switch (schema.type()) {
            case STRUCT -> applyReplacementsToStruct(target, parent, schema, input);
            case ARRAY -> applyReplacementsToArray(target, parent, schema, input);
            case STRING -> replaceString(target, parent, input);
            case BOOLEAN -> replaceBoolean(target, parent, input);
            case INT8 -> replaceByte(target, parent, input);
            case INT16 -> replaceShort(target, parent, input);
            case INT32 -> replaceInteger(target, parent, input);
            case INT64 -> replaceLong(target, parent, input);
            case FLOAT32 -> replaceFloat(target, parent, input);
            case FLOAT64 -> replaceDouble(target, parent, input);
            case null, default -> input;
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> applyReplacementsToArray(Target target, String parent, Schema schema, Object input) {
        var array = (List<Object>) input;

        return array.stream().map(it -> applyReplacements(target, parent, schema, it)).toList();
    }

    private Struct applyReplacementsToStruct(Target target, String parent, Schema schema, Object input) {
        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(schema);

        for (var field : schema.fields()) {
            var nextField = "".equals(parent) ? field.name() : parent + "." + field.name();
            newStruct.put(field.name(), applyReplacements(target, nextField, field.schema(), currentStruct.get(field)));
        }

        return newStruct;
    }

    private Object replaceString(Target target, String field, Object input) {
        return resolveMapping(target, field, input, it -> it, "");
    }

    private Object replaceBoolean(Target target, String field, Object input) {
        return resolveMapping(target, field, input, Boolean::parseBoolean, Boolean.FALSE);
    }

    private Object replaceByte(Target target, String field, Object input) {
        return resolveMapping(target, field, input, Byte::parseByte, (byte) 0);
    }

    private Object replaceShort(Target target, String field, Object input) {
        return resolveMapping(target, field, input, Short::parseShort, (short) 0);
    }

    private Object replaceInteger(Target target, String field, Object input) {
        return resolveMapping(target, field, input, Integer::parseInt, 0);
    }

    private Object replaceLong(Target target, String field, Object input) {
        return resolveMapping(target, field, input, Long::parseLong, (long) 0);
    }

    private Object replaceFloat(Target target, String field, Object input) {
        return resolveMapping(target, field, input, Float::parseFloat, (float) 0);
    }

    private Object replaceDouble(Target target, String field, Object input) {
        return resolveMapping(target, field, input, Double::parseDouble, (double) 0);
    }

    private Object resolveMapping(Target target, String field, Object input, Function<String, Object> mapping, Object defaultValue) {
        var replacement = switch (target) {
            case VALUE -> valueFieldReplacements.get(field);
            case KEY -> keyFieldReplacements.get(field);
        };

        if (replacement == null) {
            return input;
        }

        var mappedReplacement = switch (target) {
            case VALUE -> valueFieldMappedReplacements
                    .computeIfAbsent(field, ignored -> resolveReplacement(replacement, mapping, defaultValue));
            case KEY -> keyFieldMappedReplacements
                    .computeIfAbsent(field, ignored -> resolveReplacement(replacement, mapping, defaultValue));
        };

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
}
