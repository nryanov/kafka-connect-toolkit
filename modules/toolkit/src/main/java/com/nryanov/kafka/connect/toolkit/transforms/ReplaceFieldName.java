package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.domain.common.SchemaCopyUtil;
import com.nryanov.kafka.connect.toolkit.transforms.domain.trie.PrefixTrie;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.nryanov.kafka.connect.toolkit.transforms.domain.common.ConfigParser.parseCommaSeparatedPairs;
import static com.nryanov.kafka.connect.toolkit.transforms.domain.common.ConfigParser.parseCommaSeparatedSingleValues;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ReplaceFieldName<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
    private final static String EXCLUDE = "exclude";
    private final static String REPLACE = "replace";
    private final static String INCLUDE = "include";
    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            EXCLUDE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Fields to exclude from the resulting struct. This takes precedence over the include list."
                    )
                    .define(
                            REPLACE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Field rename mappings"
                    ).define(
                            INCLUDE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Fields to include. If specified, only the named fields will be included in the resulting struct."
                    );

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

    private Triplet triplet;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);

        var excludeFields = parseCommaSeparatedSingleValues(config, EXCLUDE);
        var replaceFields = parseCommaSeparatedPairs(config, REPLACE);
        var includeFields = parseCommaSeparatedSingleValues(config, INCLUDE);
        var includePrefixTrie = new PrefixTrie();
        includePrefixTrie.build(includeFields);
        triplet = new Triplet(excludeFields, replaceFields, includePrefixTrie);
    }

    protected Schema applyMappingToSchema(String parent, Schema source) {
        return switch (source.type()) {
            case ARRAY -> {
                var mappedSchema = applyMappingToSchema(parent, source.valueSchema());
                var arrayBuilder = SchemaBuilder.array(mappedSchema).name(source.name());
                yield SchemaCopyUtil.copySchemaBasics(source, arrayBuilder).build();
            }
            case STRUCT -> applyMappingsToStruct(parent, source);
            case null, default -> source;
        };
    }

    private Schema applyMappingsToStruct(String parent, Schema struct) {
        var copiedSchema = SchemaCopyUtil.copySchemaBasics(struct);

        for (var field : struct.fields()) {
            var fullPath = triplet.fullPath(parent, field);

            if (triplet.shouldExclude(fullPath)) {
                continue;
            }

            if (triplet.shouldNotInclude(fullPath)) {
                continue;
            }

            var mappedName = triplet.replacedFieldName(fullPath, field.name());
            copiedSchema.field(mappedName, applyMappingToSchema(fullPath, field.schema()));
        }

        return copiedSchema.build();
    }

    protected Object copyValuesToNewSchema(String parent, Schema source, Schema target, Object input) {
        if (input == null) {
            return null;
        }

        return switch (source.type()) {
            case ARRAY -> copyArray(parent, source.valueSchema(), target.valueSchema(), input);
            case STRUCT -> copyStruct(parent, source, target, input);
            case null, default -> input;
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> copyArray(String parent, Schema source, Schema target, Object input) {
        var inputObjects = (List<Object>) input;

        return inputObjects.stream().map(it -> copyValuesToNewSchema(parent, source, target, it)).toList();
    }

    private Struct copyStruct(String parent, Schema source, Schema target, Object input) {
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

            newStruct.put(mappedName, copyValuesToNewSchema(fullPath, currentSchema, targetSchema, currentValue));
        }

        return newStruct;
    }

    public static class Key<R extends ConnectRecord<R>> extends ReplaceFieldName<R> {
        @Override
        protected Object key(R record, Schema updatedSchema) {
            return copyValuesToNewSchema("", record.keySchema(), updatedSchema, record.key());
        }

        @Override
        protected Schema keySchema(R record) {
            return applyMappingToSchema("", record.keySchema());
        }

        @Override
        protected boolean shouldProcess(R record) {
            return record.keySchema() != null;
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ReplaceFieldName<R> {
        @Override
        protected Object value(R record, Schema updatedSchema) {
            return copyValuesToNewSchema("", record.valueSchema(), updatedSchema, record.value());
        }

        @Override
        protected Schema valueSchema(R record) {
            return applyMappingToSchema("", record.valueSchema());
        }

        @Override
        protected boolean shouldProcess(R record) {
            return record.valueSchema() != null;
        }
    }
}
