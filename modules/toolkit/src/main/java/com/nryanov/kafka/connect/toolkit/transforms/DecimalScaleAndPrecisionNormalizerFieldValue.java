package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.SchemaCopyUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class DecimalScaleAndPrecisionNormalizerFieldValue<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String DECIMAL_PRECISION_PROPERTY = "connect.decimal.precision";
    private final static String DECIMAL_SCALE_PROPERTY = "scale";

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

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
        if (source == null) {
            return null;
        }

        return switch (source.type()) {
            case ARRAY -> {
                var mappedSchema = applyMappingToSchema(source.valueSchema());
                var arrayBuilder = SchemaBuilder.array(mappedSchema).name(source.name());
                yield SchemaCopyUtil.copySchemaBasics(source, arrayBuilder).build();
            }
            case STRUCT -> applyMappingsToStruct(source);
            case BYTES -> {
                if (Decimal.LOGICAL_NAME.equals(source.name())) {
                    yield normalizeDecimalSchema(source);
                }

                yield source;
            }
            case null, default -> source;
        };
    }

    private Schema applyMappingsToStruct(Schema struct) {
        var copiedSchema = SchemaCopyUtil.copySchemaBasics(struct);

        for (var field : struct.fields()) {
            copiedSchema.field(field.name(), applyMappingToSchema(field.schema()));
        }

        return copiedSchema.build();
    }

    private Schema normalizeDecimalSchema(Schema decimal) {
        var newDecimalSchema = SchemaCopyUtil.copySchemaBasics(decimal);

        // todo: use undefined value from config
        var currentScale = Integer.parseInt(decimal.parameters().getOrDefault(DECIMAL_SCALE_PROPERTY, "0"));
        var currentPrecision = Integer.parseInt(decimal.parameters().getOrDefault(DECIMAL_PRECISION_PROPERTY, "0"));

        // todo: if values are undefined then do not update
        newDecimalSchema.parameter(DECIMAL_SCALE_PROPERTY, "3");
        newDecimalSchema.parameter(DECIMAL_PRECISION_PROPERTY, "10");

        if (decimal.isOptional()) {
            newDecimalSchema.optional();
        }

        return newDecimalSchema.build();
    }

    private Object copyValuesToNewSchema(Schema source, Schema target, Object input) {
        if (input == null) {
            return null;
        }

        return switch (source.type()) {
            case ARRAY -> copyArray(source.valueSchema(), target.valueSchema(), input);
            case STRUCT -> copyStruct(source, target, input);
            case BYTES -> {
                if (Decimal.LOGICAL_NAME.equals(source.name())) {
                    yield copyDecimal(source, target, input);
                }

                yield source;
            }
            case null, default -> input;
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> copyArray(Schema source, Schema target, Object input) {
        var inputObjects = (List<Object>) input;

        return inputObjects.stream().map(it -> copyValuesToNewSchema(source, target, it)).toList();
    }

    private Struct copyStruct(Schema source, Schema target, Object input) {
        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(target);

        for (var field : source.fields()) {
            var currentValue = currentStruct.get(field);
            var currentSchema = field.schema();
            var targetSchema = target.field(field.name()).schema();

            newStruct.put(field, copyValuesToNewSchema(currentSchema, targetSchema, currentValue));
        }

        return newStruct;
    }

    private Object copyDecimal(Schema source, Schema target, Object input) {
        var currentDecimal = (BigDecimal) input;

        var updatedScale = Integer.parseInt(target.parameters().getOrDefault(DECIMAL_SCALE_PROPERTY, "0"));
        var updatedPrecision = Integer.parseInt(target.parameters().getOrDefault(DECIMAL_PRECISION_PROPERTY, "0"));

        var mathContext = new MathContext(updatedPrecision);
        return currentDecimal.round(mathContext).setScale(updatedScale, RoundingMode.FLOOR);
    }
}
