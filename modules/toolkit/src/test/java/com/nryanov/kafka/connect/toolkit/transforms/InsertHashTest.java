package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class InsertHashTest {
    @Test
    public void correctlyHandleNullPayload() {
        var transform = new InsertHash.Key<SinkRecord>();
        transform.configure(Map.of(
                "input.field", "field_1",
                "output.field", "hash",
                "algorithm", "md5"
        ));

        var schema = Schema.STRING_SCHEMA;

        var record = new SinkRecord("topic", 1, schema, null, null, null, 0L);
        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void correctlyHandleNullSchema() {
        var transform = new InsertHash.Key<SinkRecord>();
        transform.configure(Map.of(
                "input.field", "field_1",
                "output.field", "hash",
                "algorithm", "md5"
        ));

        var record = new SinkRecord("topic", 1, null, "value", null, null, 0L);
        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void calculateHashInValue() {
        var transform = new InsertHash.Value<SinkRecord>();
        transform.configure(Map.of(
                "input.field", "field_1",
                "output.field", "hash",
                "algorithm", "md5"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .build();

        var struct = new Struct(schema).put("field_1", "field_1_value").put("field_2", "field_2_value");
        var record = new SinkRecord("topic", 1, null, null, struct.schema(), struct, 0L);

        var result = transform.apply(record);

        var resultValueStruct = requireStruct(result.value(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .field("hash", SchemaBuilder.string().optional().defaultValue(null).build())
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("field_1", "field_1_value")
                .put("field_2", "field_2_value")
                .put("hash", "78BFB84E6F4CB79B3C0E36DA36A96094");

        assertEquals(expectedSchema, resultValueStruct.schema());
        assertEquals(expectedStruct, resultValueStruct);
    }

    @Test
    public void calculateHashInKey() {
        var transform = new InsertHash.Key<SinkRecord>();
        transform.configure(Map.of(
                "input.field", "field_1",
                "output.field", "hash",
                "algorithm", "md5"
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
                .field("hash", SchemaBuilder.string().optional().defaultValue(null).build())
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("field_1", "field_1_value")
                .put("field_2", "field_2_value")
                .put("hash", "78BFB84E6F4CB79B3C0E36DA36A96094");

        assertEquals(expectedSchema, resultKeyStruct.schema());
        assertEquals(expectedStruct, resultKeyStruct);
    }

    @Test
    public void calculateHashFromNestedField() {
        var transform = new InsertHash.Key<SinkRecord>();
        transform.configure(Map.of(
                "input.field", "struct.inner_struct_b",
                "output.field", "hash",
                "algorithm", "md5"
        ));

        var nestedArrayStructSchema = SchemaBuilder
                .struct()
                .field("inner_array_a", Schema.OPTIONAL_STRING_SCHEMA)
                .field("inner_array_b", Schema.STRING_SCHEMA)
                .field("inner_array_c", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        var nestedStructSchema = SchemaBuilder
                .struct()
                .field("inner_struct_a", Schema.OPTIONAL_INT64_SCHEMA)
                .field("inner_struct_b", Schema.OPTIONAL_STRING_SCHEMA)
                .field("inner_struct_c", Schema.INT32_SCHEMA)
                .optional()
                .build();
        var schema = SchemaBuilder
                .struct()
                .field("plain", Schema.STRING_SCHEMA)
                .field("array", SchemaBuilder.array(nestedArrayStructSchema).build())
                .field("struct", nestedStructSchema)
                .build();

        var nestedArrayStruct1 = new Struct(nestedArrayStructSchema)
                .put("inner_array_a", null)
                .put("inner_array_b", "inner_array_b_value_1")
                .put("inner_array_c", "inner_array_c_value");
        var nestedArrayStruct2 = new Struct(nestedArrayStructSchema)
                .put("inner_array_a", "inner_array_a_value")
                .put("inner_array_b", "inner_array_b_value_2")
                .put("inner_array_c", null);
        var nestedStruct = new Struct(nestedStructSchema)
                .put("inner_struct_a", 64L)
                .put("inner_struct_b", "inner_struct_b_value")
                .put("inner_struct_c", 32);
        var struct = new Struct(schema)
                .put("plain", "plain_value")
                .put("array", List.of(nestedArrayStruct1, nestedArrayStruct2))
                .put("struct", nestedStruct);

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);
        var result = transform.apply(record);

        var expectedSchema = SchemaBuilder
                .struct()
                .field("plain", Schema.STRING_SCHEMA)
                .field("array", SchemaBuilder.array(nestedArrayStructSchema).build())
                .field("struct", nestedStructSchema)
                .field("hash", SchemaBuilder.string().optional().defaultValue(null).build())
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("plain", "plain_value")
                .put("array", List.of(nestedArrayStruct1, nestedArrayStruct2))
                .put("struct", nestedStruct)
                .put("hash", "2B46B6E5ED6DF3383B87D40DF5B79258");

        assertEquals(expectedSchema, result.keySchema());
        assertEquals(expectedStruct, result.key());
    }
}
