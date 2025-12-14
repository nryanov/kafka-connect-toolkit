package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class ReplaceFieldName<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String KEY_EXCLUDE = "key.exclude";
    private final static String KEY_REPLACE = "key.replace";
    private final static String KEY_INCLUDE = "key.include";
    private final static String VALUE_EXCLUDE = "value.exclude";
    private final static String VALUE_REPLACE = "value.replace";
    private final static String VALUE_INCLUDE = "value.include";
    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            KEY_EXCLUDE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Fields to exclude from the resulting Struct or Map. This takes precedence over the include list."
                    )
                    .define(
                            KEY_REPLACE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Field rename mappings"
                    ).define(
                            KEY_INCLUDE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Fields to include. If specified, only the named fields will be included in the resulting Struct or Map."
                    ).define(
                            VALUE_EXCLUDE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Fields to exclude from the resulting Struct or Map. This takes precedence over the include list."
                    )
                    .define(
                            VALUE_REPLACE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Field rename mappings"
                    ).define(
                            VALUE_INCLUDE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Fields to include. If specified, only the named fields will be included in the resulting Struct or Map."
                    );

    private static class PrefixTrie {
        final Map<String, PrefixTrie> trie = new HashMap<>();

        public PrefixTrie() {}

        void build(Collection<String> includeFields) {
            includeFields.forEach(it -> {
                var splits = it.split("[.]");

                var current = this;
                for (var split : splits) {
                    var next = current.trie.get(split);
                    if (next == null) {
                        next = new PrefixTrie();
                        current.trie.put(split, next);
                    }

                    current = next;
                }
            });
        }

        boolean isEmpty() {
            return trie.isEmpty();
        }

        boolean shouldInclude(String value) {
            var splits = value.split("[.]");

            var current = this;
            var i = 0;

            for (; i < splits.length && current.trie.containsKey(splits[i]); i++) {
                current = current.trie.get(splits[i]);
            }

            // specific field should be included
            if (i == splits.length) {
                return true;
            }

            // all child fields should be included or this filed and it's child should be excluded
            return current.trie.isEmpty();
        }
    }

    private record Triplet(
            Set<String> excludeFields,
            Map<String, String> replaceFields,
            PrefixTrie includeFieldsTrie
    ) {
        boolean shouldExclude(String fullPath) {
            return excludeFields.contains(fullPath);
        }

        boolean shouldNotInclude(String fullPath) {
            return !includeFieldsTrie.isEmpty() && !includeFieldsTrie.shouldInclude(fullPath);
        }

        String fullPath(String parent, Field field) {
            var prefix = "".equals(parent) ? "" : parent + ".";
            return prefix + field.name();
        }

        String replacedFieldName(String fullPath, String defaultValue) {
            return replaceFields.getOrDefault(fullPath, defaultValue);
        }
    }

    private final Set<String> keyExcludeFields = new HashSet<>();
    private final Map<String, String> keyReplaceFields = new HashMap<>();
    private final Set<String> keyIncludeFields = new HashSet<>();

    private Triplet keyTriplet;

    private final Set<String> valueExcludeFields = new HashSet<>();
    private final Map<String, String> valueReplaceFields = new HashMap<>();
    private final Set<String> valueIncludeFields = new HashSet<>();

    private Triplet valueTriplet;

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

        parseCommaSeparatedSingleValues(config, KEY_EXCLUDE, keyExcludeFields);
        parseCommaSeparatedSingleValues(config, VALUE_EXCLUDE, valueExcludeFields);

        parseCommaSeparatedPairs(config, KEY_REPLACE, keyReplaceFields);
        parseCommaSeparatedPairs(config, VALUE_REPLACE, valueReplaceFields);

        parseCommaSeparatedSingleValues(config, KEY_INCLUDE, keyIncludeFields);
        parseCommaSeparatedSingleValues(config, VALUE_INCLUDE, valueIncludeFields);

        var keyIncludePrefixTrie = new PrefixTrie();
        keyIncludePrefixTrie.build(keyIncludeFields);

        var valueIncludePrefixTrie = new PrefixTrie();
        valueIncludePrefixTrie.build(valueIncludeFields);

        keyTriplet = new Triplet(keyExcludeFields, keyReplaceFields, keyIncludePrefixTrie);
        valueTriplet = new Triplet(valueExcludeFields, valueReplaceFields, valueIncludePrefixTrie);
    }

    private static void parseCommaSeparatedSingleValues(AbstractConfig config, String name, Set<String> target) {
        Arrays
                .asList(Objects.requireNonNullElse(config.getString(name), "").split(","))
                .forEach(it -> {
                    if (!it.isBlank()) {
                        target.add(it);
                    }
                });
    }

    private static void parseCommaSeparatedPairs(AbstractConfig config, String name, Map<String, String> target) {
        Arrays.stream(Objects.requireNonNullElse(config.getString(name), "")
                        .split(","))
                .forEach(it -> {
                    var pair = it.split(":");

                    if (pair.length == 2) {
                        var from = pair[0];
                        var to = pair[1];

                        target.put(from, to);
                    }
                });
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        var initialParentPath = "";

        var mappedKeySchema = applyMappingToSchema(keyTriplet, initialParentPath, record.keySchema());
        var mappedValueSchema = applyMappingToSchema(valueTriplet, initialParentPath, record.valueSchema());

        var mappedKey = copyValuesToNewSchema(keyTriplet, initialParentPath, record.keySchema(), mappedKeySchema, record.key());
        var mappedValue = copyValuesToNewSchema(valueTriplet, initialParentPath, record.valueSchema(), mappedValueSchema, record.value());

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                mappedKeySchema,
                mappedKey,
                mappedValueSchema,
                mappedValue,
                record.timestamp()
        );
    }

    private Schema applyMappingToSchema(Triplet triplet, String parent, Schema source) {
        return switch (source.type()) {
            case ARRAY -> {
                var mappedSchema = applyMappingToSchema(triplet, parent, source.valueSchema());
                var arrayBuilder = SchemaBuilder.array(mappedSchema).name(source.name());
                yield SchemaUtil.copySchemaBasics(source, arrayBuilder).build();
            }
            case STRUCT -> applyMappingsToStruct(triplet, parent, source);
            case null, default -> source;
        };
    }

    private Schema applyMappingsToStruct(Triplet triplet, String parent, Schema struct) {
        var copiedSchema = SchemaUtil.copySchemaBasics(struct);

        for (var field : struct.fields()) {
            var fullPath = triplet.fullPath(parent, field);

            if (triplet.shouldExclude(fullPath)) {
                continue;
            }

            if (triplet.shouldNotInclude(fullPath)) {
                continue;
            }

            var mappedName = triplet.replacedFieldName(fullPath, field.name());

            copiedSchema.field(mappedName, applyMappingToSchema(triplet, fullPath, field.schema()));
        }

        return copiedSchema.build();
    }

    private Object copyValuesToNewSchema(Triplet triplet, String parent, Schema source, Schema target, Object input) {
        return switch (source.type()) {
            case ARRAY -> copyArray(triplet, parent, source.valueSchema(), target.valueSchema(), input);
            case STRUCT -> copyStruct(triplet, parent, source, target, input);
            case null, default -> input;
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> copyArray(Triplet triplet, String parent, Schema source, Schema target, Object input) {
        var inputObjects = (List<Object>) input;

        return inputObjects.stream().map(it -> copyValuesToNewSchema(triplet, parent, source, target, it)).toList();
    }

    private Struct copyStruct(Triplet triplet, String parent, Schema source, Schema target, Object input) {
        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(target);

        for (var field : source.fields()) {
            var fullPath = triplet.fullPath(parent, field);

            if (triplet.shouldExclude(fullPath)) {
                continue;
            }

            if (triplet.shouldNotInclude(fullPath)) {
                continue;
            }

            var mappedName = triplet.replacedFieldName(fullPath, field.name());

            var currentValue = currentStruct.get(field);
            var currentSchema = field.schema();
            var targetSchema = target.field(mappedName).schema();

            newStruct.put(mappedName, copyValuesToNewSchema(triplet, fullPath, currentSchema, targetSchema, currentValue));
        }

        return newStruct;
    }
}
