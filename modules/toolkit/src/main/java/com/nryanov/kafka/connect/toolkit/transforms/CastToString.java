package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.domain.common.ConfigParser;
import com.nryanov.kafka.connect.toolkit.transforms.domain.common.SchemaCopyUtil;
import com.nryanov.kafka.connect.toolkit.transforms.domain.model.FieldFilter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class CastToString<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
    private final static String FIELDS = "fields";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "List of fields which should be modified. Allowed values: NULL (no fields will be modified), * (all fields), concrete list of fields"
                    );

    private FieldFilter fieldsFilter;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);

        var fieldsRaw = config.getString(FIELDS);

        if (fieldsRaw == null) {
            throw new DataException("Empty `fields` parameter");
        } else if ("*".equals(fieldsRaw)) {
            fieldsFilter = new FieldFilter.All();
        } else {
            fieldsFilter = new FieldFilter.Subset(ConfigParser.parseCommaSeparatedSingleValues(fieldsRaw));
        }
    }

    protected Schema applyMappingToSchema(String parent, Schema source) {
        if (source == null) {
            return null;
        }

        return switch (source.type()) {
            case ARRAY -> {
                var mappedSchema = applyMappingToSchema(parent, source.valueSchema());
                var arrayBuilder = SchemaBuilder.array(mappedSchema).name(source.name());
                yield SchemaCopyUtil.copySchemaBasics(source, arrayBuilder).build();
            }
            case STRUCT -> applyMappingsToStruct(parent, source);
            // except bytes
            case FLOAT64, FLOAT32, BOOLEAN, INT8, INT16, INT32, INT64 -> convertToStringSchema(parent, source);
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

    private Schema convertToStringSchema(String parent, Schema input) {
        var shouldCopy = fieldsFilter.shouldApply(parent);
        if (!shouldCopy) {
            return input;
        }

        var newSchema = SchemaBuilder.string();
        return SchemaCopyUtil.copySchemaBasics(input, newSchema).build();
    }

    protected Object copyValuesToNewSchema(String parent, Schema source, Schema target, Object input) {
        if (input == null) {
            return null;
        }

        return switch (source.type()) {
            case ARRAY -> copyArray(parent, source.valueSchema(), target.valueSchema(), input);
            case STRUCT -> copyStruct(parent, source, target, input);
            // except bytes
            case FLOAT64, FLOAT32, BOOLEAN, INT8, INT16, INT32, INT64 -> convertToString(parent, input);
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
            var currentValue = currentStruct.get(field);
            var currentSchema = field.schema();
            var targetSchema = target.field(field.name()).schema();

            var nextField = "".equals(parent) ? field.name() : parent + "." + field.name();
            newStruct.put(field.name(), copyValuesToNewSchema(nextField, currentSchema, targetSchema, currentValue));
        }

        return newStruct;
    }

    private Object convertToString(String parent, Object input) {
        var shouldCopy = fieldsFilter.shouldApply(parent);
        if (!shouldCopy) {
            return input;
        }

        return input.toString();
    }
}
