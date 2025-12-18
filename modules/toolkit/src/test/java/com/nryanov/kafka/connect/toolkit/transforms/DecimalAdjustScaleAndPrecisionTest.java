package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class DecimalAdjustScaleAndPrecisionTest {
    @Test
    public void doNotChangePrecisionAndScaleWhenAllModesAreNone() {
        var transform = new DecimalAdjustScaleAndPrecision<SinkRecord>();
        transform.configure(Map.of(
                "precision.value", 10,
                "scale.value", 5
        ));

        var scale = 5;
        var precision = 10;

        var decimalSchema = Decimal.builder(scale).parameter("connect.decimal.precision", String.valueOf(precision)).build();
        var schema = SchemaBuilder.struct().field("decimal", decimalSchema).build();

        var decimalKey = new BigDecimal(BigInteger.valueOf(123321), scale, new MathContext(precision));
        var decimalValue = new BigDecimal(BigInteger.valueOf(321123), scale, new MathContext(precision));

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
}
