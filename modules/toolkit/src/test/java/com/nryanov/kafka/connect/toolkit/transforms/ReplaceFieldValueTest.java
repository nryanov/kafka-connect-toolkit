package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReplaceFieldValueTest {
    @Test
    public void correctlyHandleNullPayload() {
        var transform = new ReplaceFieldValue.Key<SinkRecord>();
        transform.configure(Map.of(
                "exclude", "b,c"
        ));

        var record = new SinkRecord("topic", 1, null, null, null, null, 0L);
        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void replaceFieldInKey() {
        var transform = new ReplaceFieldValue.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "b:123,c:custom_value"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 123)
                .put("c", "custom_value");

        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void replaceFieldInValue() {
        var transform = new ReplaceFieldValue.Value<SinkRecord>();
        transform.configure(Map.of(
                "fields", "b:123,c:custom_value"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var record = new SinkRecord("topic", 1, null, null, struct.schema(), struct, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.value(), "test");

        var expectedStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 123)
                .put("c", "custom_value");

        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void replaceNestedFields() {
        var transform = new ReplaceFieldValue.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "b.inner_a:123,a:custom_value"
        ));

        var nestedSchema = SchemaBuilder
                .struct()
                .field("inner_a", Schema.STRING_SCHEMA)
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", nestedSchema)
                .build();

        var nestedStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", nestedStruct);

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedNestedStruct = new Struct(nestedSchema)
                .put("inner_a", "123")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var expectedStruct = new Struct(schema)
                .put("a", "custom_value")
                .put("b", expectedNestedStruct);

        assertEquals(expectedStruct, resultStruct);
    }
}
