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
import java.util.Map;

public class DecimalScaleAndPrecisionNormalizerFieldValueTest {
    @Test
    public void foo() {
        var transform = new DecimalScaleAndPrecisionNormalizerFieldValue<SinkRecord>();
        transform.configure(Map.of());

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

        System.out.println(result.keySchema().field("decimal").schema().parameters());
        System.out.println(result.valueSchema().field("decimal").schema().parameters());

        System.out.println(record.key());
        System.out.println(record.value());

        System.out.println("NEW KEY: " + result.key());
        System.out.println("NEW VALUE: " + result.value());
    }
}
