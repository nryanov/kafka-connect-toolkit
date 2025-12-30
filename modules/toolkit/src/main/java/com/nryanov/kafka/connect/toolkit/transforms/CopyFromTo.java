package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.core.CacheableTransform;
import com.nryanov.kafka.connect.toolkit.core.common.ConfigParser;
import com.nryanov.kafka.connect.toolkit.core.model.FieldFilter;
import com.nryanov.kafka.connect.toolkit.core.common.SchemaCopyUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class CopyFromTo<R extends ConnectRecord<R>> extends CacheableTransform<R> {
    private final static String FIELDS = "fields";
    private final static String DEFAULT_SUFFIX = "suffix";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Comma separated list of fields in source part which should be copied to target"
                    )
                    .define(
                            DEFAULT_SUFFIX,
                            ConfigDef.Type.STRING,
                            "_copy",
                            ConfigDef.Importance.MEDIUM,
                            "Default suffix which should be used for re-naming if field should be copied (e.g. it's a leaf field) but concrete renaming for it is not defined"
                    );

    private Map<String, String> mappings;
    private FieldFilter filter;
    private String suffix;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        var config = new AbstractConfig(CONFIG_DEF, configs);
        var mappingsRaw = config.getString(FIELDS);

        if (mappingsRaw != null && !"*".equals(mappingsRaw)) {
            mappings = ConfigParser.parseCommaSeparatedPairs(config, FIELDS);
            filter = new FieldFilter.Subset(mappings.keySet());
        } else if ("*".equals(mappingsRaw)) {
            mappings = new HashMap<>();
            filter = new FieldFilter.All();
        } else {
            throw new DataException("Empty `fields` parameter");
        }
        suffix = config.getString(DEFAULT_SUFFIX);
    }


    protected Schema extractSchemaPatch(String parent, Schema source) {
        return switch (source.type()) {
            case ARRAY -> {
                var mappedSchema = extractSchemaPatch(parent, source.valueSchema());
                var arrayBuilder = SchemaBuilder.array(mappedSchema).name(source.name());
                yield SchemaCopyUtil.copySchemaBasics(source, arrayBuilder).build();
            }
            case STRUCT -> extractSchemaPatchFromStruct(parent, source);
            case null, default -> source;
        };
    }

    protected Schema extractSchemaPatchFromStruct(String parent, Schema source) {
        var copiedSchema = SchemaCopyUtil.copySchemaBasics(source);

        for (var field : source.fields()) {
            var fieldFullPath = "".equals(parent) ? field.name() : parent + "." + field.name();

            if (filter.shouldApply(fieldFullPath)) {
                var mappedFieldName = mappings.getOrDefault(fieldFullPath, field.name() + suffix);
                copiedSchema.field(mappedFieldName, extractSchemaPatch(fieldFullPath, field.schema()));
            }
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
    protected List<Object> copyArray(String parent, Schema source, Schema target, Object input) {
        var inputObjects = (List<Object>) input;

        return inputObjects.stream().map(it -> copyValuesToNewSchema(parent, source, target, it)).toList();
    }

    protected Struct copyStruct(String parent, Schema source, Schema target, Object input) {
        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(target);

        for (var field : source.fields()) {
            var fieldFullPath = "".equals(parent) ? field.name() : parent + "." + field.name();

            if (filter.shouldApply(fieldFullPath)) {
                var mappedFieldName = mappings.getOrDefault(fieldFullPath, field.name() + suffix);

                var currentValue = currentStruct.get(field);
                var currentSchema = field.schema();
                var targetSchema = target.field(mappedFieldName).schema();

                newStruct.put(mappedFieldName, copyValuesToNewSchema(fieldFullPath, currentSchema, targetSchema, currentValue));
            }
        }

        return newStruct;
    }

    protected Schema mergeSchemas(Schema first, Schema second) {
        var mergedSchema = SchemaCopyUtil.copySchemaBasics(first);

        for (var field : first.fields()) {
            mergedSchema.field(field.name(), field.schema());
        }

        for (var field : second.fields()) {
            mergedSchema.field(field.name(), field.schema());
        }

        return mergedSchema.build();
    }

    protected Struct mergeStructs(Schema schema, Object first, Object second) {
        var firstStruct = requireStruct(first, "struct required");
        var secondStruct = requireStruct(second, "struct required");
        var mergedStruct = new Struct(schema);

        for (var field : firstStruct.schema().fields()) {
            var currentValue = firstStruct.get(field);
            mergedStruct.put(field.name(), currentValue);
        }

        for (var field : secondStruct.schema().fields()) {
            var currentValue = secondStruct.get(field);
            mergedStruct.put(field.name(), currentValue);
        }

        return mergedStruct;
    }

    public static class KeyToValue<R extends ConnectRecord<R>> extends CopyFromTo<R> {
        @Override
        public R apply(R record) {
            if (record == null) {
                return null;
            }

            if (!shouldProcess(record)) {
                return record;
            }

            var initialParentPath = "";

            var schemaPatch = extractSchemaPatch(initialParentPath, record.keySchema());
            var structPatch = copyValuesToNewSchema(initialParentPath, record.keySchema(), schemaPatch, record.key());
            var mergedValueSchema = getOrCompute(record.valueSchema(), () -> mergeSchemas(record.valueSchema(), schemaPatch));
            var mergedValueStruct = mergeStructs(mergedValueSchema, record.value(), structPatch);

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    mergedValueSchema,
                    mergedValueStruct,
                    record.timestamp()
            );
        }

        @Override
        protected boolean shouldProcess(R record) {
            return record.keySchema() != null && record.valueSchema() != null;
        }
    }

    public static class ValueToKey<R extends ConnectRecord<R>> extends CopyFromTo<R> {
        @Override
        public R apply(R record) {
            if (record == null) {
                return null;
            }

            if (!shouldProcess(record)) {
                return record;
            }

            var initialParentPath = "";

            var schemaPatch = extractSchemaPatch(initialParentPath, record.valueSchema());
            var structPatch = copyValuesToNewSchema(initialParentPath, record.valueSchema(), schemaPatch, record.value());
            var mergedKeySchema = getOrCompute(record.keySchema(), () -> mergeSchemas(record.keySchema(), schemaPatch));
            var mergedKeyStruct = mergeStructs(mergedKeySchema, record.key(), structPatch);

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    mergedKeySchema,
                    mergedKeyStruct,
                    record.valueSchema(),
                    record.value(),
                    record.timestamp()
            );
        }

        @Override
        protected boolean shouldProcess(R record) {
            return record.keySchema() != null && record.valueSchema() != null;
        }
    }
}
