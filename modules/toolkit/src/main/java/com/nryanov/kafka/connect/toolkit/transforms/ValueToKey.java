package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.ConfigParser;
import com.nryanov.kafka.connect.toolkit.transforms.common.FieldFiler;
import com.nryanov.kafka.connect.toolkit.transforms.common.SchemaCopyUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class ValueToKey<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String FIELDS = "fields";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Comma separated list of fields in value which should be copied to key. Fields may be nested, but in result they will be in top of structure of key"
                    );

    private Map<String, String> mappings;
    private FieldFiler.Subset filer;

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
        mappings = ConfigParser.parseCommaSeparatedPairs(config, FIELDS);
        filer = new FieldFiler.Subset(mappings.keySet());
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        var initialParentPath = "";

        var schemaPatch = extractSchemaPatch(initialParentPath, record.valueSchema());
        var structPatch = copyValuesToNewSchema(initialParentPath, record.valueSchema(), schemaPatch, record.value());
        var mergesKeySchema = mergeSchemas(record.keySchema(), schemaPatch);
        var mergedKeyStruct = mergeStructs(mergesKeySchema, record.key(), structPatch);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                mergesKeySchema,
                mergedKeyStruct,
                record.valueSchema(),
                record.value(),
                record.timestamp()
        );
    }

    private Schema extractSchemaPatch(String parent, Schema source) {
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

    private Schema extractSchemaPatchFromStruct(String parent, Schema source) {
        var copiedSchema = SchemaCopyUtil.copySchemaBasics(source);

        for (var field : source.fields()) {
            var fieldFullPath = "".equals(parent) ? field.name() : parent + "." + field.name();

            if (filer.shouldApply(fieldFullPath)) {
                var mappedFieldName = mappings.get(fieldFullPath);
                copiedSchema.field(mappedFieldName, extractSchemaPatch(fieldFullPath, field.schema()));
            }
        }

        return copiedSchema.build();
    }

    private Object copyValuesToNewSchema(String parent, Schema source, Schema target, Object input) {
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
            var fieldFullPath = "".equals(parent) ? field.name() : parent + "." + field.name();

            if (filer.shouldApply(fieldFullPath)) {
                var mappedFieldName = mappings.get(fieldFullPath);

                var currentValue = currentStruct.get(field);
                var currentSchema = field.schema();
                var targetSchema = target.field(mappedFieldName).schema();

                newStruct.put(mappedFieldName, copyValuesToNewSchema(fieldFullPath, currentSchema, targetSchema, currentValue));
            }
        }

        return newStruct;
    }

    private Schema mergeSchemas(Schema first, Schema second) {
        var mergedSchema = SchemaCopyUtil.copySchemaBasics(first);

        for (var field : first.fields()) {
            mergedSchema.field(field.name(), field.schema());
        }

        for (var field : second.fields()) {
            mergedSchema.field(field.name(), field.schema());
        }

        return mergedSchema.build();
    }

    private Struct mergeStructs(Schema schema, Object first, Object second) {
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
}
