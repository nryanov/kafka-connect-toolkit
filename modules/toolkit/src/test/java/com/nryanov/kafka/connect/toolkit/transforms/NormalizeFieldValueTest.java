package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.DefaultRecords;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.*;

public class NormalizeFieldValueTest {
    @Test
    public void correctlyHandleNullPayload() {
        var transform = new NormalizeFieldValue.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "a:LOWER_UNDERSCORE:LOWER_HYPHEN,c.inner_a:LOWER_UNDERSCORE:LOWER_CAMEL,c.inner_c.inner_inner_a:LOWER_UNDERSCORE:LOWER_CAMEL"
        ));

        var schema = Schema.STRING_SCHEMA;

        var record = new SinkRecord("topic", 1, schema, null, null, null, 0L);
        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void correctlyHandleNullSchema() {
        var transform = new NormalizeFieldValue.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "a:LOWER_UNDERSCORE:LOWER_HYPHEN,c.inner_a:LOWER_UNDERSCORE:LOWER_CAMEL,c.inner_c.inner_inner_a:LOWER_UNDERSCORE:LOWER_CAMEL"
        ));

        var record = new SinkRecord("topic", 1, null, "value", null, null, 0L);
        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void changeCaseOfNestedFieldsInKey() {
        var transform = new NormalizeFieldValue.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "a:LOWER_UNDERSCORE:LOWER_HYPHEN,c.inner_a:LOWER_UNDERSCORE:LOWER_CAMEL,c.inner_c.inner_inner_a:LOWER_UNDERSCORE:LOWER_CAMEL"
        ));

        var keyStruct = DefaultRecords.createNestedKeyStruct();

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, null, null, 0L);

        var result = transform.apply(record);
        var keyStructResult = requireStruct(result.key(), "test");

        assertEquals("a-field-value", keyStructResult.get("a"));
        assertEquals("innerAValue", keyStructResult.getStruct("c").get("inner_a"));
        assertEquals("inner_b_value", keyStructResult.getStruct("c").get("inner_b"));
        assertEquals("innerInnerAArrayValue", requireStruct(keyStructResult.getStruct("c").getArray("inner_c").getFirst(), "test").get("inner_inner_a"));
    }

    @Test
    public void changeCaseOfPlainFieldInKey() {
        var transform = new NormalizeFieldValue.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", ":LOWER_UNDERSCORE:UPPER_UNDERSCORE"
        ));

        var keySchema = Schema.STRING_SCHEMA;
        var key = "key";

        var record = new SinkRecord("topic", 1, keySchema, key, null, null, 0L);
        var result = transform.apply(record);

        assertEquals("KEY", result.key());
    }
}
