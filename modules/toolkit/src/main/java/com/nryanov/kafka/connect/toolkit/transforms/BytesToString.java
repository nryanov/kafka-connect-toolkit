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

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class BytesToString<R extends ConnectRecord<R>> extends CacheableTransform<R> {
    private final static String FIELDS = "fields";
    private final static String CHARSET = "charset";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "List of fields in which should be modified. Allowed values: * (all fields), concrete list of fields"
                    )
                    .define(
                            CHARSET,
                            ConfigDef.Type.STRING,
                            "UTF-8",
                            ConfigDef.Importance.HIGH,
                            "Charset which should be used to decode string values. Default: UTF-8"
                    );

    private FieldFilter fieldFilter;
    private Charset charset;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        var config = new AbstractConfig(CONFIG_DEF, configs);

        var fieldsRaw = config.getString(FIELDS);

        if (fieldsRaw == null) {
            throw new DataException("Empty `fields` parameter");
        } else if ("*".equals(fieldsRaw)) {
            fieldFilter = new FieldFilter.All();
        } else {
            fieldFilter = new FieldFilter.Subset(ConfigParser.parseCommaSeparatedSingleValues(fieldsRaw));
        }

        charset = Charset.forName(config.getString(CHARSET));
    }

    protected Schema applyMappingToSchema(String field, Schema source) {
        return switch (source.type()) {
            case ARRAY -> {
                var mappedSchema = applyMappingToSchema(field, source.valueSchema());
                var arrayBuilder = SchemaBuilder.array(mappedSchema).name(source.name());
                yield SchemaCopyUtil.copySchemaBasics(source, arrayBuilder).build();
            }
            case STRUCT -> applyMappingsToStruct(field, source);
            case BYTES -> convertBytesToStringSchema(field, source);
            case null, default -> source;
        };
    }

    private Schema applyMappingsToStruct(String parent, Schema struct) {
        var copiedSchema = SchemaCopyUtil.copySchemaBasics(struct);

        for (var field : struct.fields()) {
            var nextField = "".equals(parent) ? field.name() : parent + "." + field.name();
            copiedSchema.field(field.name(), applyMappingToSchema(nextField, field.schema()));
        }

        return copiedSchema.build();
    }

    private Schema convertBytesToStringSchema(String parent, Schema input) {
        var shouldCopy = fieldFilter.shouldApply(parent);

        if (!shouldCopy) {
            return input;
        }

        var newSchema = SchemaBuilder.string();
        return SchemaCopyUtil.copySchemaBasics(input, newSchema).build();
    }

    protected Object copyValuesToNewSchema(String field, Schema source, Schema target, Object input) {
        if (input == null) {
            return null;
        }

        return switch (source.type()) {
            case ARRAY -> copyArray(field, source.valueSchema(), target.valueSchema(), input);
            case STRUCT -> copyStruct(field, source, target, input);
            case BYTES -> decodeBytesToString(field, input);
            case null, default -> input;
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> copyArray(String field, Schema source, Schema target, Object input) {
        var inputObjects = (List<Object>) input;

        return inputObjects.stream().map(it -> copyValuesToNewSchema(field, source, target, it)).toList();
    }

    private Struct copyStruct(String parent, Schema source, Schema target, Object input) {
        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(target);

        for (var field : source.fields()) {
            var currentValue = currentStruct.get(field);
            var currentSchema = field.schema();
            var targetSchema = target.field(field.name()).schema();

            var nextField = "".equals(parent) ? field.name() : parent + "." + field.name();
            newStruct.put(field.name(), copyValuesToNewSchema(nextField, currentSchema, targetSchema, currentValue));
        }

        return newStruct;
    }

    private Object decodeBytesToString(String field, Object input) {
        var shouldCopy = fieldFilter.shouldApply(field);

        if (!shouldCopy) {
            return input;
        }

        var bytes = (byte[]) input;
        return new String(bytes, charset);
    }

    public static class Key<R extends ConnectRecord<R>> extends BytesToString<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends BytesToString<R> {
        @Override
        protected Schema valueSchema(R record) {
            return applyMappingToSchema("", record.valueSchema());
        }

        @Override
        protected Object value(R record, Schema updatedSchema) {
            return copyValuesToNewSchema("", record.valueSchema(), updatedSchema, record.value());
        }

        @Override
        protected boolean shouldProcess(R record) {
            return record.valueSchema() != null;
        }
    }
}
