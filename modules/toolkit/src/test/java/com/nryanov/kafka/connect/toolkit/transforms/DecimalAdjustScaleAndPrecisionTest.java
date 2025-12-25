package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class DecimalAdjustScaleAndPrecisionTest {
    private Schema createDecimalSchema(int precision, int scale) {
        return Decimal.builder(scale).parameter("connect.decimal.precision", String.valueOf(precision)).build();
    }

    private BigDecimal createDecimalValue(int precision, int scale, long value) {
        return new BigDecimal(BigInteger.valueOf(value), scale, new MathContext(precision));
    }

    @Test
    public void doNotChangePrecisionAndScaleWhenAllModesAreNone() {
        var transform = new DecimalAdjustScaleAndPrecision.Key<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 10,
                "scale.value", 5,
                "fields", "*"
        ));

        var scale = 5;
        var precision = 10;

        var decimalSchema = createDecimalSchema(precision, scale);
        var schema = SchemaBuilder.struct().field("decimal", decimalSchema).build();

        var decimalKey = createDecimalValue(precision, scale, 123321);
        var decimalValue = createDecimalValue(precision, scale, 321123);

        var keyStruct = new Struct(schema).put("decimal", decimalKey);
        var valueStruct = new Struct(schema).put("decimal", decimalValue);

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultKeyStruct = requireStruct(result.key(), "test");
        var resultValueStruct = requireStruct(result.value(), "test");

        assertEquals(schema, result.keySchema());
        assertEquals(schema, result.valueSchema());

        assertEquals(decimalKey, resultKeyStruct.get("decimal"));
        assertEquals(decimalValue, resultValueStruct.get("decimal"));
    }

    @Test
    public void changeOnlyPrecision() {
        var transform = new DecimalAdjustScaleAndPrecision.Value<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 6,
                "scale.value", 5,
                "precision.mode", "VALUE",
                "fields", "*"
        ));

        var scale = 5;
        var precision = 10;

        var decimalSchema = createDecimalSchema(precision, scale);
        var schema = SchemaBuilder.struct().field("decimal", decimalSchema).build();
        var decimalValue = createDecimalValue(precision, scale, 321123);
        var valueStruct = new Struct(schema).put("decimal", decimalValue);

        var record = new SinkRecord("topic", 1, null, null, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var resultValueStruct = requireStruct(result.value(), "test");
        var expectedSchema = SchemaBuilder.struct().field("decimal", createDecimalSchema(6, 5)).build();

        assertEquals(expectedSchema, result.valueSchema());
        assertEquals(decimalValue, resultValueStruct.get("decimal"));
    }

    @Test
    public void changeOnlyScale() {
        var transform = new DecimalAdjustScaleAndPrecision.Value<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 10,
                "scale.value", 2,
                "scale.mode", "VALUE",
                "fields", "*"
        ));

        var scale = 5;
        var precision = 10;

        var decimalSchema = createDecimalSchema(precision, scale);
        var schema = SchemaBuilder.struct().field("decimal", decimalSchema).build();
        var decimalValue = createDecimalValue(precision, scale, 321123);
        var valueStruct = new Struct(schema).put("decimal", decimalValue);

        var record = new SinkRecord("topic", 1, null, null, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var resultValueStruct = requireStruct(result.value(), "test");
        var expectedPrecision = 10;
        var expectedScale = 2;
        var expectedSchema = SchemaBuilder.struct().field("decimal", createDecimalSchema(expectedPrecision, expectedScale)).build();
        var expectedDecimal = BigDecimal.valueOf(3.21);

        assertEquals(expectedSchema, result.valueSchema());
        assertEquals(expectedDecimal, resultValueStruct.get("decimal"));
    }

    @Test
    public void changePrecisionAndScale() {
        var transform = new DecimalAdjustScaleAndPrecision.Value<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 4,
                "scale.value", 2,
                "precision.mode", "VALUE",
                "scale.mode", "VALUE",
                "fields", "*"
        ));

        var scale = 5;
        var precision = 10;

        var decimalSchema = createDecimalSchema(precision, scale);
        var schema = SchemaBuilder.struct().field("decimal", decimalSchema).build();
        var decimalValue = createDecimalValue(precision, scale, 321123);
        var valueStruct = new Struct(schema).put("decimal", decimalValue);

        var record = new SinkRecord("topic", 1, null, null, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var resultValueStruct = requireStruct(result.value(), "test");
        var expectedPrecision = 4;
        var expectedScale = 2;
        var expectedSchema = SchemaBuilder.struct().field("decimal", createDecimalSchema(expectedPrecision, expectedScale)).build();
        var expectedDecimal = BigDecimal.valueOf(3.21);

        assertEquals(expectedSchema, result.valueSchema());
        assertEquals(expectedDecimal, resultValueStruct.get("decimal"));
    }

    @Test
    public void changePrecisionAndScaleOnlyIfCurrentValuesAreUndefined() {
        var transform = new DecimalAdjustScaleAndPrecision.Value<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 4,
                "scale.value", 2,
                "precision.mode", "IF_NOT_SET",
                "scale.mode", "IF_NOT_SET",
                "precision.undefined-value", "-1",
                "scale.undefined-value", "-1",
                "fields", "*"
        ));

        var scale = 5;
        var precision = 10;

        var decimalSchema = createDecimalSchema(-1, -1);
        var schema = SchemaBuilder.struct().field("decimal", decimalSchema).build();
        var decimalValue = createDecimalValue(precision, scale, 321123);
        var valueStruct = new Struct(schema).put("decimal", decimalValue);

        var record = new SinkRecord("topic", 1, null, null, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var resultValueStruct = requireStruct(result.value(), "test");
        var expectedPrecision = 4;
        var expectedScale = 2;
        var expectedSchema = SchemaBuilder.struct().field("decimal", createDecimalSchema(expectedPrecision, expectedScale)).build();
        var expectedDecimal = BigDecimal.valueOf(3.21);

        assertEquals(expectedSchema, result.valueSchema());
        assertEquals(expectedDecimal, resultValueStruct.get("decimal"));
    }

    @Test
    public void changePrecisionAndScaleOnlyIfCurrentValuesExceededLimits() {
        var transform = new DecimalAdjustScaleAndPrecision.Value<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 8,
                "scale.value", 3,
                "precision.mode", "LIMIT",
                "scale.mode", "LIMIT",
                "fields", "*"
        ));

        var scale = 5;
        var precision = 10;

        var decimalSchema = createDecimalSchema(precision, scale);
        var schema = SchemaBuilder.struct().field("decimal", decimalSchema).build();
        var decimalValue = createDecimalValue(precision, scale, 321123); // 3.21123
        var valueStruct = new Struct(schema).put("decimal", decimalValue);

        var record = new SinkRecord("topic", 1, null, null, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var resultValueStruct = requireStruct(result.value(), "test");
        var expectedPrecision = 8;
        var expectedScale = 3;
        var expectedSchema = SchemaBuilder.struct().field("decimal", createDecimalSchema(expectedPrecision, expectedScale)).build();
        var expectedDecimal = BigDecimal.valueOf(3.211);

        assertEquals(expectedSchema, result.valueSchema());
        assertEquals(expectedDecimal, resultValueStruct.get("decimal"));
    }

    @Test
    public void doNotChangeScaleIfModeIsScaleToZeroButValueIsNotZero() {
        var transform = new DecimalAdjustScaleAndPrecision.Key<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 10,
                "scale.value", 5,
                "precision.mode", "LIMIT",
                "scale.mode", "NONE",
                "scale.zero-mode", "VALUE",
                "fields", "*"
        ));

        var scale = 5;
        var precision = 10;

        var decimalSchema = createDecimalSchema(precision, scale);
        var schema = SchemaBuilder.struct().field("decimal", decimalSchema).build();
        var decimalValue = createDecimalValue(precision, scale, 321123);
        var valueStruct = new Struct(schema).put("decimal", decimalValue);

        var record = new SinkRecord("topic", 1, null, null, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var resultValueStruct = requireStruct(result.value(), "test");
        var expectedPrecision = 10;
        var expectedScale = 5;
        var expectedSchema = SchemaBuilder.struct().field("decimal", createDecimalSchema(expectedPrecision, expectedScale)).build();
        var expectedDecimal = decimalValue;

        assertEquals(expectedSchema, result.valueSchema());
        assertEquals(expectedDecimal, resultValueStruct.get("decimal"));
    }

    @Test
    public void changeScaleIfModeIsScaleToZeroAndValueIsZero() {
        var transform = new DecimalAdjustScaleAndPrecision.Value<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 10,
                "scale.value", 5,
                "precision.mode", "LIMIT",
                "scale.mode", "NONE",
                "scale.zero-mode", "VALUE",
                "fields", "*"
        ));

        var scale = 0;
        var precision = 10;

        var decimalSchema = createDecimalSchema(precision, scale);
        var schema = SchemaBuilder.struct().field("decimal", decimalSchema).build();
        var decimalValue = createDecimalValue(precision, scale, 321123); // 321123
        var valueStruct = new Struct(schema).put("decimal", decimalValue);

        var record = new SinkRecord("topic", 1, null, null, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var resultValueStruct = requireStruct(result.value(), "test");
        var expectedPrecision = 10;
        var expectedScale = 5;
        var expectedSchema = SchemaBuilder.struct().field("decimal", createDecimalSchema(expectedPrecision, expectedScale)).build();
        var expectedDecimal = decimalValue.round(new MathContext(expectedPrecision)).setScale(expectedScale, RoundingMode.FLOOR);

        assertEquals(expectedSchema, result.valueSchema());
        assertEquals(expectedDecimal, resultValueStruct.get("decimal"));
    }

    @Test
    public void changeScaleIfCurrentScaleIsNegativeAndNegativeModeIsValue() {
        var transform = new DecimalAdjustScaleAndPrecision.Value<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 10,
                "scale.value", -3,
                "scale.mode", "NONE",
                "scale.negative-mode", "VALUE",
                "fields", "*"
        ));

        var scale = -5;
        var precision = 10;

        var decimalSchema = createDecimalSchema(precision, scale);
        var schema = SchemaBuilder.struct().field("decimal", decimalSchema).build();
        var decimalValue = createDecimalValue(precision, scale, 321123); // 321123
        var valueStruct = new Struct(schema).put("decimal", decimalValue);

        var record = new SinkRecord("topic", 1, null, null, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var resultValueStruct = requireStruct(result.value(), "test");
        var expectedPrecision = 10;
        var expectedScale = -3;
        var expectedSchema = SchemaBuilder.struct().field("decimal", createDecimalSchema(expectedPrecision, expectedScale)).build();
        var expectedDecimal = decimalValue.round(new MathContext(expectedPrecision)).setScale(expectedScale, RoundingMode.FLOOR);

        assertEquals(expectedSchema, result.valueSchema());
        assertEquals(expectedDecimal, resultValueStruct.get("decimal"));
    }

    @Test
    public void changePrecisionAndScaleOnlyForSelectedFields() {
        var transform = new DecimalAdjustScaleAndPrecision.Key<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 10,
                "scale.value", 5,
                "scale.mode", "VALUE",
                "precision.mode", "VALUE",
                "fields", "nested.nested_decimal" // only selected leaf-field
        ));

        var scale = 4;
        var precision = 8;

        var initialDecimalSchema = createDecimalSchema(precision, scale);
        var schema = SchemaBuilder
                .struct()
                .field("decimal", initialDecimalSchema)
                .field(
                        "nested",
                        SchemaBuilder
                                .struct()
                                .field("nested_decimal", initialDecimalSchema)
                                .field(
                                        "nested_level_2",
                                        SchemaBuilder
                                                .struct()
                                                .field("nested_level_2_decimal_1", initialDecimalSchema)
                                                .field("nested_level_2_decimal_2", initialDecimalSchema)
                                                .optional()
                                                .build()
                                )
                                .optional()
                                .build()
                )
                .build();

        var decimalKey = createDecimalValue(precision, scale, 123321);

        var keyStruct = new Struct(schema).put("decimal", decimalKey);

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, null, null, 0L);

        var result = transform.apply(record);

        var expectedPrecision = 10;
        var expectedScale = 5;
        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("decimal", initialDecimalSchema)
                .field(
                        "nested",
                        SchemaBuilder
                                .struct()
                                .field("nested_decimal", createDecimalSchema(expectedPrecision, expectedScale))
                                .field(
                                        "nested_level_2",
                                        SchemaBuilder
                                                .struct()
                                                .field("nested_level_2_decimal_1", initialDecimalSchema)
                                                .field("nested_level_2_decimal_2", initialDecimalSchema)
                                                .optional()
                                                .build()
                                )
                                .optional()
                                .build()
                )
                .build();

        assertEquals(expectedKeySchema, result.keySchema());
    }

    @Test
    public void changePrecisionAndScaleOnlyForSelectedFieldsUnderParentStruct() {
        var transform = new DecimalAdjustScaleAndPrecision.Value<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 10,
                "scale.value", 5,
                "scale.mode", "VALUE",
                "precision.mode", "VALUE",
                "fields", "nested.nested_level_2" // all nested fields under selected parent
        ));

        var scale = 4;
        var precision = 8;

        var initialDecimalSchema = createDecimalSchema(precision, scale);
        var schema = SchemaBuilder
                .struct()
                .field("decimal", initialDecimalSchema)
                .field(
                        "nested",
                        SchemaBuilder
                                .struct()
                                .field("nested_decimal", initialDecimalSchema)
                                .field(
                                        "nested_level_2",
                                        SchemaBuilder
                                                .struct()
                                                .field("nested_level_2_decimal_1", initialDecimalSchema)
                                                .field("nested_level_2_decimal_2", initialDecimalSchema)
                                                .optional()
                                                .build()
                                )
                                .optional()
                                .build()
                )
                .build();

        var decimalValue = createDecimalValue(precision, scale, 321123);

        var valueStruct = new Struct(schema).put("decimal", decimalValue);

        var record = new SinkRecord("topic", 1, null, null, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var expectedPrecision = 10;
        var expectedScale = 5;

        var expectedValueSchema = SchemaBuilder
                .struct()
                .field("decimal", initialDecimalSchema)
                .field(
                        "nested",
                        SchemaBuilder
                                .struct()
                                .field("nested_decimal", initialDecimalSchema)
                                .field(
                                        "nested_level_2",
                                        SchemaBuilder
                                                .struct()
                                                .field("nested_level_2_decimal_1", createDecimalSchema(expectedPrecision, expectedScale))
                                                .field("nested_level_2_decimal_2", createDecimalSchema(expectedPrecision, expectedScale))
                                                .optional()
                                                .build()
                                )
                                .optional()
                                .build()
                )
                .build();

        assertEquals(expectedValueSchema, result.valueSchema());
    }

    @Test
    public void changePrecisionAndScaleOnlyForKeyPart() {
        var transform = new DecimalAdjustScaleAndPrecision.Key<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 10,
                "scale.value", 5,
                "scale.mode", "VALUE",
                "precision.mode", "VALUE",
                "fields", "nested.nested_decimal" // only selected leaf-field
        ));

        var scale = 4;
        var precision = 8;

        var initialDecimalSchema = createDecimalSchema(precision, scale);
        var schema = SchemaBuilder
                .struct()
                .field("decimal", initialDecimalSchema)
                .field(
                        "nested",
                        SchemaBuilder
                                .struct()
                                .field("nested_decimal", initialDecimalSchema)
                                .field(
                                        "nested_level_2",
                                        SchemaBuilder
                                                .struct()
                                                .field("nested_level_2_decimal_1", initialDecimalSchema)
                                                .field("nested_level_2_decimal_2", initialDecimalSchema)
                                                .optional()
                                                .build()
                                )
                                .optional()
                                .build()
                )
                .build();

        var decimalKey = createDecimalValue(precision, scale, 123321);
        var decimalValue = createDecimalValue(precision, scale, 321123);

        var keyStruct = new Struct(schema).put("decimal", decimalKey);
        var valueStruct = new Struct(schema).put("decimal", decimalValue);

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var expectedPrecision = 10;
        var expectedScale = 5;
        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("decimal", initialDecimalSchema)
                .field(
                        "nested",
                        SchemaBuilder
                                .struct()
                                .field("nested_decimal", createDecimalSchema(expectedPrecision, expectedScale))
                                .field(
                                        "nested_level_2",
                                        SchemaBuilder
                                                .struct()
                                                .field("nested_level_2_decimal_1", initialDecimalSchema)
                                                .field("nested_level_2_decimal_2", initialDecimalSchema)
                                                .optional()
                                                .build()
                                )
                                .optional()
                                .build()
                )
                .build();

        assertEquals(expectedKeySchema, result.keySchema());
    }

    @Test
    public void changePrecisionAndScaleForAllDecimalFields() {
        var transform = new DecimalAdjustScaleAndPrecision.Key<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 10,
                "scale.value", 5,
                "scale.mode", "VALUE",
                "precision.mode", "VALUE",
                "fields", "*"
        ));

        var scale = 4;
        var precision = 8;

        var initialDecimalSchema = createDecimalSchema(precision, scale);
        var schema = SchemaBuilder
                .struct()
                .field("decimal", initialDecimalSchema)
                .field(
                        "nested",
                        SchemaBuilder
                                .struct()
                                .field("nested_decimal", initialDecimalSchema)
                                .field(
                                        "nested_level_2",
                                        SchemaBuilder
                                                .struct()
                                                .field("nested_level_2_decimal_1", initialDecimalSchema)
                                                .field("nested_level_2_decimal_2", initialDecimalSchema)
                                                .optional()
                                                .build()
                                )
                                .optional()
                                .build()
                )
                .build();

        var decimalKey = createDecimalValue(precision, scale, 123321);

        var struct = new Struct(schema).put("decimal", decimalKey);

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);

        var expectedPrecision = 10;
        var expectedScale = 5;
        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("decimal", createDecimalSchema(expectedPrecision, expectedScale))
                .field(
                        "nested",
                        SchemaBuilder
                                .struct()
                                .field("nested_decimal", createDecimalSchema(expectedPrecision, expectedScale))
                                .field(
                                        "nested_level_2",
                                        SchemaBuilder
                                                .struct()
                                                .field("nested_level_2_decimal_1", createDecimalSchema(expectedPrecision, expectedScale))
                                                .field("nested_level_2_decimal_2", createDecimalSchema(expectedPrecision, expectedScale))
                                                .optional()
                                                .build()
                                )
                                .optional()
                                .build()
                )
                .build();

        assertEquals(expectedKeySchema, result.keySchema());
    }
}
