package com.nryanov.kafka.connect.toolkit.transforms;

import com.google.common.base.CaseFormat;
import com.nryanov.kafka.connect.toolkit.core.AbstractBaseTransform;
import com.nryanov.kafka.connect.toolkit.core.common.SchemaCopyUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class NormalizeFieldName<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
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
    public void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);
        initialCase = CaseFormat.valueOf(Objects.requireNonNull(config.getString(INITIAL_CASE), "Empty case.initial config"));
        targetCase = CaseFormat.valueOf(Objects.requireNonNull(config.getString(TARGET_CASE), "Empty case.target config"));
    }

    protected Schema applyMappingToSchema(Schema source) {
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

    protected Object copyValuesToNewSchema(Schema source, Schema target, Object input) {
        if (input == null) {
            return null;
        }

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

    public static class Key<R extends ConnectRecord<R>> extends NormalizeFieldName<R> {
        @Override
        protected Object key(R record, Schema updatedSchema) {
            return copyValuesToNewSchema(record.keySchema(), updatedSchema, record.key());
        }

        @Override
        protected Schema keySchema(R record) {
            return applyMappingToSchema(record.keySchema());
        }

        @Override
        protected boolean shouldProcess(R record) {
            return record.keySchema() != null;
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends NormalizeFieldName<R> {
        @Override
        protected Object value(R record, Schema updatedSchema) {
            return copyValuesToNewSchema(record.valueSchema(), updatedSchema, record.value());
        }

        @Override
        protected Schema valueSchema(R record) {
            return applyMappingToSchema(record.valueSchema());
        }

        @Override
        protected boolean shouldProcess(R record) {
            return record.valueSchema() != null;
        }
    }
}
