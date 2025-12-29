package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNull;

public class DropSchemalessTest {
    @Test
    public void dropRecordIfKeySchemaIsNull() {
        var transform = new DropSchemaless.Key<SinkRecord>();
        transform.configure(Map.of());

        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .build();

        var struct = new Struct(schema).put("a", "a_field_value");

        var record = new SinkRecord("topic", 1, null, struct, null, null, 0L);

        var result = transform.apply(record);

        assertNull(result);
    }

    @Test
    public void dropRecordIfValueSchemaIsNull() {
        var transform = new DropSchemaless.Value<SinkRecord>();
        transform.configure(Map.of());

        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .build();

        var struct = new Struct(schema).put("a", "a_field_value");

        var record = new SinkRecord("topic", 1, null, null, null, struct, 0L);

        var result = transform.apply(record);

        assertNull(result);
    }
}
