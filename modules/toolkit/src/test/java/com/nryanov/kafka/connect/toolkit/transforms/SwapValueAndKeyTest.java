package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SwapValueAndKeyTest {
    @Test
    public void swapKeyAndValue() {
        var transform = new SwapValueAndKey<SinkRecord>();
        transform.configure(Map.of());

        var keySchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .build();

        var valueSchema = SchemaBuilder
                .struct()
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var keyStruct = new Struct(keySchema)
                .put("a", "a_field_value");

        var valueStruct = new Struct(valueSchema)
                .put("b", 1)
                .put("c", "c_field_value");

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultValueStruct = requireStruct(result.value(), "test");
        var resultKeyStruct = requireStruct(result.key(), "test");

        // swap schemas
        assertEquals(keySchema, result.valueSchema());
        assertEquals(valueSchema, result.keySchema());
        // swap payloads
        assertEquals(keyStruct, resultValueStruct);
        assertEquals(valueStruct, resultKeyStruct);
    }
}
