package com.nryanov.kafka.connect.toolkit.transforms;

import com.google.common.base.CaseFormat;
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
import java.util.Objects;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class NormalizeFieldName<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String INITIAL_CASE = "case.initial";
    private final static String TARGET_CASE = "case.target";
    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            INITIAL_CASE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Initial case of field names which should be considered for change"
                    )
                    .define(
                            TARGET_CASE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Target case of field names"
                    );

    private CaseFormat initialCase;
    private CaseFormat targetCase;

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
        initialCase = CaseFormat.valueOf(Objects.requireNonNull(config.getString(INITIAL_CASE), "Empty case.initial config"));
        targetCase = CaseFormat.valueOf(Objects.requireNonNull(config.getString(TARGET_CASE), "Empty case.target config"));
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        var mappedKeySchema = applyMappingToSchema(record.keySchema());
        var mappedValueSchema = applyMappingToSchema(record.valueSchema());

        var mappedKey = copyValuesToNewSchema(record.keySchema(), mappedKeySchema, record.key());
        var mappedValue = copyValuesToNewSchema(record.valueSchema(), mappedValueSchema, record.value());

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

    private Schema applyMappingToSchema(Schema source) {
        return switch (source.type()) {
            case ARRAY -> {
                var mappedSchema = applyMappingToSchema(source.valueSchema());
                var arrayBuilder = SchemaBuilder.array(mappedSchema).name(source.name());
                yield SchemaCopyUtil.copySchemaBasics(source, arrayBuilder).build();
            }
            case STRUCT -> applyMappingsToStruct(source);
            case null, default -> source;
        };
    }

    private Schema applyMappingsToStruct(Schema struct) {
        var copiedSchema = SchemaCopyUtil.copySchemaBasics(struct);

        for (var field : struct.fields()) {
            var name = initialCase.to(targetCase, field.name());
            copiedSchema.field(name, applyMappingToSchema(field.schema()));
        }

        return copiedSchema.build();
    }

    private Object copyValuesToNewSchema(Schema source, Schema target, Object input) {
        return switch (source.type()) {
            case ARRAY -> copyArray(source.valueSchema(), target.valueSchema(), input);
            case STRUCT -> copyStruct(source, target, input);
            case null, default -> input;
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> copyArray(Schema source, Schema target, Object input) {
        if (input == null) {
            return null;
        }

        var inputObjects = (List<Object>) input;

        return inputObjects.stream().map(it -> copyValuesToNewSchema(source, target, it)).toList();
    }

    private Struct copyStruct(Schema source, Schema target, Object input) {
        if (input == null) {
            return null;
        }

        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(target);

        for (var field : source.fields()) {
            var currentValue = currentStruct.get(field);
            var currentSchema = field.schema();
            var fieldName = initialCase.to(targetCase, field.name());
            var targetSchema = target.field(fieldName).schema();

            newStruct.put(fieldName, copyValuesToNewSchema(currentSchema, targetSchema, currentValue));
        }

        return newStruct;
    }
}
