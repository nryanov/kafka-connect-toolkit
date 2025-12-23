package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConcatFieldsTest {
    @Test
    public void concatPlainFieldsInKey() {
        var transform = new ConcatFields.Key<SinkRecord>();
        transform.configure(Map.of(
                "input.fields", "field_1,field_2",
                "output.field", "concatenated",
                "delimiter", "_"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .build();

        var struct = new Struct(schema).put("field_1", "field_1_value").put("field_2", "field_2_value");
        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);

        var resultKeyStruct = requireStruct(result.key(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .field("concatenated", SchemaBuilder.string().optional().defaultValue(null).build())
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("field_1", "field_1_value")
                .put("field_2", "field_2_value")
                .put("concatenated", "field_1_value_field_2_value");

        assertEquals(expectedSchema, resultKeyStruct.schema());
        assertEquals(expectedStruct, resultKeyStruct);
    }
}
