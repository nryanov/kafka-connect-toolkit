package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SetNullTest {
    @Test
    public void setValueAsNull() {
        var transform = new SetNull.Value<SinkRecord>();
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

        assertNotNull(result.key());
        assertNotNull(result.keySchema());

        assertNull(result.value());
        assertNull(result.valueSchema());
    }

    @Test
    public void setKeyAsNull() {
        var transform = new SetNull.Key<SinkRecord>();
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

        assertNull(result.key());
        assertNull(result.keySchema());

        assertNotNull(result.value());
        assertNotNull(result.valueSchema());
    }
}
