package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReplaceFieldValueTest {
    @Test
    public void excludeField() {
        var transform = new ReplaceFieldValue<SinkRecord>();
        transform.configure(Map.of(
                "key.fields", "b:123",
                "value.fields", "c:custom_value"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var keyStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var valueStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultValueStruct = requireStruct(result.value(), "test");
        var resultKeyStruct = requireStruct(result.key(), "test");

        var expectedKeyStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 123)
                .put("c", "c_field_value");

        var expectedValueStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "custom_value");

        assertEquals(expectedKeyStruct, resultKeyStruct);
        assertEquals(expectedValueStruct, resultValueStruct);
    }
}
