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

public class CastToStringTest {
    @Test
    public void correctlyHandleNullPayload() {
        var transform = new CastToString.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field_1,field_2"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .build();

        var record = new SinkRecord("topic", 1, schema, null, null, null, 0L);

        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void castFieldsToStringInKey() {
        var transform = new CastToString.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field_1,field_2"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.INT32_SCHEMA)
                .field("field_2", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .build();
        var struct = new Struct(schema).put("field_1", 32).put("field_2", 64.0d);

        var record = new SinkRecord("topic", 1, schema, struct, null, null, 0L);
        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("field_1", "32")
                .put("field_2", "64.0");

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void castFieldsToStringInValue() {
        var transform = new CastToString.Value<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field_1,field_2"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.INT32_SCHEMA)
                .field("field_2", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .build();
        var struct = new Struct(schema).put("field_1", 32).put("field_2", 64.0d);

        var record = new SinkRecord("topic", 1, null, null, schema, struct, 0L);
        var result = transform.apply(record);
        var resultStruct = requireStruct(result.value(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("field_1", "32")
                .put("field_2", "64.0");

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void castToStringNestedFields() {
        var transform = new CastToString.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "plain,array.inner_array_b,struct.inner_struct_a"
        ));

        var nestedArrayStructSchema = SchemaBuilder
                .struct()
                .field("inner_array_a", Schema.OPTIONAL_INT64_SCHEMA)
                .field("inner_array_b", Schema.INT32_SCHEMA)
                .field("inner_array_c", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .build();
        var nestedStructSchema = SchemaBuilder
                .struct()
                .field("inner_struct_a", Schema.BOOLEAN_SCHEMA)
                .field("inner_struct_b", Schema.OPTIONAL_STRING_SCHEMA)
                .field("inner_struct_c", Schema.INT32_SCHEMA)
                .optional()
                .build();
        var schema = SchemaBuilder
                .struct()
                .field("plain", Schema.INT32_SCHEMA)
                .field("array", SchemaBuilder.array(nestedArrayStructSchema).build())
                .field("struct", nestedStructSchema)
                .build();

        var nestedArrayStruct1 = new Struct(nestedArrayStructSchema)
                .put("inner_array_a", null)
                .put("inner_array_b", 32)
                .put("inner_array_c", true);
        var nestedArrayStruct2 = new Struct(nestedArrayStructSchema)
                .put("inner_array_a", 1L)
                .put("inner_array_b", 23)
                .put("inner_array_c", null);
        var nestedStruct = new Struct(nestedStructSchema)
                .put("inner_struct_a", false)
                .put("inner_struct_b", "inner_struct_b_value")
                .put("inner_struct_c", 123);
        var struct = new Struct(schema)
                .put("plain", 321)
                .put("array", List.of(nestedArrayStruct1, nestedArrayStruct2))
                .put("struct", nestedStruct);

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);
        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedNestedArrayStructSchema = SchemaBuilder
                .struct()
                .field("inner_array_a", Schema.OPTIONAL_INT64_SCHEMA)
                .field("inner_array_b", Schema.STRING_SCHEMA)
                .field("inner_array_c", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .build();
        var expectedNestedStructSchema = SchemaBuilder
                .struct()
                .field("inner_struct_a", Schema.STRING_SCHEMA)
                .field("inner_struct_b", Schema.OPTIONAL_STRING_SCHEMA)
                .field("inner_struct_c", Schema.INT32_SCHEMA)
                .optional()
                .build();
        var expectedSchema = SchemaBuilder
                .struct()
                .field("plain", Schema.STRING_SCHEMA)
                .field("array", SchemaBuilder.array(expectedNestedArrayStructSchema).build())
                .field("struct", expectedNestedStructSchema)
                .build();

        var expectedNestedArrayStruct1 = new Struct(expectedNestedArrayStructSchema)
                .put("inner_array_a", null)
                .put("inner_array_b", "32")
                .put("inner_array_c", true);
        var expectedNestedArrayStruct2 = new Struct(expectedNestedArrayStructSchema)
                .put("inner_array_a", 1L)
                .put("inner_array_b", "23")
                .put("inner_array_c", null);
        var expectedNestedStruct = new Struct(expectedNestedStructSchema)
                .put("inner_struct_a", "false")
                .put("inner_struct_b", "inner_struct_b_value")
                .put("inner_struct_c", 123);
        var expectedStruct = new Struct(expectedSchema)
                .put("plain", "321")
                .put("array", List.of(expectedNestedArrayStruct1, expectedNestedArrayStruct2))
                .put("struct", expectedNestedStruct);

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }
}
