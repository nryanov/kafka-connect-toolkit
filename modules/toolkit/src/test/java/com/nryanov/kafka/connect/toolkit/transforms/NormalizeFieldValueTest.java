package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.DefaultRecords;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.*;

public class NormalizeFieldValueTest {
    @Test
    public void changeCaseOfNestedFields() {
        var transform = new NormalizeFieldValue<SinkRecord>();
        transform.configure(Map.of(
                "key.fields", "a:LOWER_UNDERSCORE:LOWER_HYPHEN,c.inner_a:LOWER_UNDERSCORE:LOWER_CAMEL,c.inner_c.inner_inner_a:LOWER_UNDERSCORE:LOWER_CAMEL",
                "value.fields", "b.inner_b.inner_inner_b:UPPER_UNDERSCORE:LOWER_UNDERSCORE"
        ));

        var keyStruct = DefaultRecords.createNestedKeyStruct();
        var valueStruct = DefaultRecords.createNestedValueStruct();

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var keyStructResult = requireStruct(result.key(), "test");
        var valueStructResult = requireStruct(result.value(), "test");

        assertEquals("a-field-value", keyStructResult.get("a"));
        assertEquals("innerAValue", keyStructResult.getStruct("c").get("inner_a"));
        assertEquals("inner_b_value", keyStructResult.getStruct("c").get("inner_b"));
        assertEquals("innerInnerAArrayValue", requireStruct(keyStructResult.getStruct("c").getArray("inner_c").getFirst(), "test").get("inner_inner_a"));

        assertEquals("a_value", valueStructResult.get("a"));
        assertEquals("inner_inner_a_value", valueStructResult.getStruct("b").getStruct("inner_b").get("inner_inner_a"));
        assertEquals(List.of("first", "second", "third"), valueStructResult.getStruct("b").getStruct("inner_b").get("inner_inner_b"));
    }

    @Test
    public void changeCaseOfPlainField() {
        var transform = new NormalizeFieldValue<SinkRecord>();
        transform.configure(Map.of(
                "key.fields", ":LOWER_UNDERSCORE:UPPER_UNDERSCORE"
        ));

        var keySchema = Schema.STRING_SCHEMA;
        var key = "key";
        var valueSchema = Schema.STRING_SCHEMA;
        var value = "value";

        var record = new SinkRecord("topic", 1, keySchema, key, valueSchema, value, 0L);

        var result = transform.apply(record);

        assertEquals("KEY", result.key());
        assertEquals("value", result.value());
    }
}
