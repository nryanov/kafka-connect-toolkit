package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.ConfigParser;
import com.nryanov.kafka.connect.toolkit.transforms.common.FieldFiler;
import com.nryanov.kafka.connect.toolkit.transforms.common.SchemaCopyUtil;
import com.nryanov.kafka.connect.toolkit.transforms.common.Target;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class BytesToString<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String KEY_FIELDS = "key.fields";
    private final static String VALUE_FIELDS = "value.fields";
    private final static String CHARSET = "charset";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            KEY_FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "List of fields in key-part which should be modified. Allowed values: NULL (no fields will be modified), * (all fields), concrete list of fields"
                    )
                    .define(
                            VALUE_FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "List of fields in key-part which should be modified. Allowed values: NULL (no fields will be modified), * (all fields), concrete list of fields"
                    ).define(
                            CHARSET,
                            ConfigDef.Type.STRING,
                            "UTF-8",
                            ConfigDef.Importance.HIGH,
                            "Charset which should be used to decode string values. Default: UTF-8"
                    );

    private FieldFiler keyFieldFilter;
    private FieldFiler valueFieldFilter;
    private Charset charset;

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

        var keyFieldsRaw = config.getString(KEY_FIELDS);
        var valueFieldsRaw = config.getString(VALUE_FIELDS);

        if (keyFieldsRaw == null) {
            keyFieldFilter = new FieldFiler.None();
        } else if ("*".equals(keyFieldsRaw)) {
            keyFieldFilter = new FieldFiler.All();
        } else {
            keyFieldFilter = new FieldFiler.Subset(ConfigParser.parseCommaSeparatedSingleValues(keyFieldsRaw));
        }

        if (valueFieldsRaw == null) {
            valueFieldFilter = new FieldFiler.None();
        } else if ("*".equals(valueFieldsRaw)) {
            valueFieldFilter = new FieldFiler.All();
        } else {
            valueFieldFilter = new FieldFiler.Subset(ConfigParser.parseCommaSeparatedSingleValues(valueFieldsRaw));
        }

        charset = Charset.forName(config.getString(CHARSET));
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        var initialParentPath = "";

        var mappedKeySchema = record.keySchema();
        var mappedKey = record.key();

        if (keyFieldFilter.enabled()) {
            mappedKeySchema = applyMappingToSchema(Target.KEY, initialParentPath, record.keySchema());
            mappedKey = copyValuesToNewSchema(Target.KEY, initialParentPath, record.keySchema(), mappedKeySchema, record.key());
        }

        var mappedValue = record.value();
        var mappedValueSchema = record.valueSchema();

        if (valueFieldFilter.enabled()) {
            mappedValueSchema = applyMappingToSchema(Target.VALUE, initialParentPath, record.valueSchema());
            mappedValue = copyValuesToNewSchema(Target.VALUE, initialParentPath, record.valueSchema(), mappedValueSchema, record.value());
        }

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

    private Schema applyMappingToSchema(Target targetType, String parent, Schema source) {
        if (source == null) {
            return null;
        }

        return switch (source.type()) {
            case ARRAY -> {
                var mappedSchema = applyMappingToSchema(targetType, parent, source.valueSchema());
                var arrayBuilder = SchemaBuilder.array(mappedSchema).name(source.name());
                yield SchemaCopyUtil.copySchemaBasics(source, arrayBuilder).build();
            }
            case STRUCT -> applyMappingsToStruct(targetType, parent, source);
            case BYTES -> convertBytesToStringSchema(targetType, parent, source);
            case null, default -> source;
        };
    }

    private Schema applyMappingsToStruct(Target targetType, String parent, Schema struct) {
        var copiedSchema = SchemaCopyUtil.copySchemaBasics(struct);

        for (var field : struct.fields()) {
            var nextField = "".equals(parent) ? field.name() : parent + "." + field.name();
            copiedSchema.field(field.name(), applyMappingToSchema(targetType, nextField, field.schema()));
        }

        return copiedSchema.build();
    }

    private Schema convertBytesToStringSchema(Target targetType, String parent, Schema input) {
        var shouldCopy = switch (targetType) {
            case VALUE -> valueFieldFilter.shouldApply(parent);
            case KEY -> keyFieldFilter.shouldApply(parent);
        };

        if (!shouldCopy) {
            return input;
        }

        var newSchema = SchemaBuilder.string();
        return SchemaCopyUtil.copySchemaBasics(input, newSchema).build();
    }

    private Object copyValuesToNewSchema(Target targetType, String parent, Schema source, Schema target, Object input) {
        if (input == null) {
            return null;
        }

        return switch (source.type()) {
            case ARRAY -> copyArray(targetType, parent, source.valueSchema(), target.valueSchema(), input);
            case STRUCT -> copyStruct(targetType, parent, source, target, input);
            case BYTES -> decodeBytesToString(targetType, parent, input);
            case null, default -> input;
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> copyArray(Target targetType, String parent, Schema source, Schema target, Object input) {
        var inputObjects = (List<Object>) input;

        return inputObjects.stream().map(it -> copyValuesToNewSchema(targetType, parent, source, target, it)).toList();
    }

    private Struct copyStruct(Target targetType, String parent, Schema source, Schema target, Object input) {
        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(target);

        for (var field : source.fields()) {
            var currentValue = currentStruct.get(field);
            var currentSchema = field.schema();
            var targetSchema = target.field(field.name()).schema();

            var nextField = "".equals(parent) ? field.name() : parent + "." + field.name();
            newStruct.put(field.name(), copyValuesToNewSchema(targetType, nextField, currentSchema, targetSchema, currentValue));
        }

        return newStruct;
    }

    private Object decodeBytesToString(Target targetType, String parent, Object input) {
        var shouldCopy = switch (targetType) {
            case VALUE -> valueFieldFilter.shouldApply(parent);
            case KEY -> keyFieldFilter.shouldApply(parent);
        };

        if (!shouldCopy) {
            return input;
        }

        var bytes = (byte[]) input;
        return new String(bytes, charset);
    }
}
