package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ConcatFieldsTest {
    @Test
    public void correctlyHandleNullPayload() {
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

        var record = new SinkRecord("topic", 1, schema, null, null, null, 0L);
        var result = transform.apply(record);

        var expectedSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .field("concatenated", SchemaBuilder.string().optional().defaultValue(null).build())
                .build();

        assertEquals(expectedSchema, result.keySchema());
        assertNull(result.key());
    }

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

    @Test
    public void concatPlainFieldsInValue() {
        var transform = new ConcatFields.Value<SinkRecord>();
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
        var record = new SinkRecord("topic", 1, null, null, struct.schema(), struct, 0L);

        var result = transform.apply(record);

        var resultValueStruct = requireStruct(result.value(), "test");

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

        assertEquals(expectedSchema, resultValueStruct.schema());
        assertEquals(expectedStruct, resultValueStruct);
    }

    @Test
    public void preserveOrder() {
        var transform = new ConcatFields.Key<SinkRecord>();
        transform.configure(Map.of(
                "input.fields", "field_2,field_1",
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
                .put("concatenated", "field_2_value_field_1_value");

        assertEquals(expectedSchema, resultKeyStruct.schema());
        assertEquals(expectedStruct, resultKeyStruct);
    }

    @Test
    public void replaceNullValues() {
        var transform = new ConcatFields.Key<SinkRecord>();
        transform.configure(Map.of(
                "input.fields", "field_1,field_2",
                "output.field", "concatenated",
                "input.fields.null-replacement", "NULL",
                "delimiter", "_"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.OPTIONAL_STRING_SCHEMA)
                .field("field_2", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        var struct = new Struct(schema).put("field_1", "field_1_value").put("field_2", null);
        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);

        var resultKeyStruct = requireStruct(result.key(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.OPTIONAL_STRING_SCHEMA)
                .field("field_2", Schema.OPTIONAL_STRING_SCHEMA)
                .field("concatenated", SchemaBuilder.string().optional().defaultValue(null).build())
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("field_1", "field_1_value")
                .put("field_2", null)
                .put("concatenated", "field_1_value_NULL");

        assertEquals(expectedSchema, resultKeyStruct.schema());
        assertEquals(expectedStruct, resultKeyStruct);
    }

    @Test
    public void preserveOrderOfFieldsForConcatenation() {
        var transform = new ConcatFields.Key<SinkRecord>();
        transform.configure(Map.of(
                "input.fields", "field_2,field_1",
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
                .put("concatenated", "field_2_value_field_1_value");

        assertEquals(expectedSchema, resultKeyStruct.schema());
        assertEquals(expectedStruct, resultKeyStruct);
    }

    @Test
    public void concatNestedFields() {
        var transform = new ConcatFields.Key<SinkRecord>();
        transform.configure(Map.of(
                "input.fields", "array.inner_array_a,struct.inner_struct_b",
                "input.fields.null-replacement", "NULL",
                "output.field", "concatenated",
                "delimiter", "_"
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
                .field("concatenated", SchemaBuilder.string().optional().defaultValue(null).build())
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("plain", "plain_value")
                .put("array", List.of(nestedArrayStruct1, nestedArrayStruct2))
                .put("struct", nestedStruct)
                .put("concatenated", "NULL_inner_array_a_value_inner_struct_b_value");

        assertEquals(expectedSchema, result.keySchema());
        assertEquals(expectedStruct, result.key());
    }
}
