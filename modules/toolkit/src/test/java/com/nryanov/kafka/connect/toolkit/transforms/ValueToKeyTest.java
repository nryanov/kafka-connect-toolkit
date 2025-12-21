package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ValueToKeyTest {
    @Test
    public void addFieldsToKeyFromValue() {
        var transform = new ValueToKey<SinkRecord>();
        transform.configure(Map.of(
                "fields", "b:copied_b,c:copied_c"
        ));

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

        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("copied_b", Schema.INT32_SCHEMA)
                .field("copied_c", Schema.STRING_SCHEMA)
                .build();

        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("a", "a_field_value")
                .put("copied_b", 1)
                .put("copied_c", "c_field_value");

        assertEquals(expectedKeySchema, result.keySchema());
        assertEquals(expectedKeyStruct, result.key());
    }
}
