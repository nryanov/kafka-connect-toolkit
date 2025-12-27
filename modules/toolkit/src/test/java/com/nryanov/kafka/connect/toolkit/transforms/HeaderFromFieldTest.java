package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HeaderFromFieldTest {
    @Test
    public void correctlyHandleNullPayload() {
        var transform = new HeaderFromField.Key<SinkRecord>();
        transform.configure(Map.of(
                "mappings", "field_1:header_from_field_1,field_2:header_from_field_2"
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
    public void headerFromFieldsInKey() {
        var transform = new HeaderFromField.Key<SinkRecord>();
        transform.configure(Map.of(
                "mappings", "field_1:header_from_field_1,field_2:header_from_field_2"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.INT32_SCHEMA)
                .field("field_2", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .build();
        var struct = new Struct(schema).put("field_1", 32).put("field_2", 64.0d);

        var record = new SinkRecord("topic", 1, schema, struct, null, null, 0L);
        var result = transform.apply(record);

        var expectedHeaders = new ConnectHeaders();
        expectedHeaders.add("header_from_field_1", 32, Schema.INT32_SCHEMA);
        expectedHeaders.add("header_from_field_2", 64.0d, Schema.OPTIONAL_FLOAT64_SCHEMA);

        // struct should be the same
        assertEquals(struct, result.key());
        assertEquals(expectedHeaders, result.headers());
    }

    @Test
    public void headerFromFieldsInValue() {
        var transform = new HeaderFromField.Value<SinkRecord>();
        transform.configure(Map.of(
                "mappings", "field_1:header_from_field_1,field_2:header_from_field_2"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.INT32_SCHEMA)
                .field("field_2", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .build();
        var struct = new Struct(schema).put("field_1", 32).put("field_2", 64.0d);

        var record = new SinkRecord("topic", 1, null, null, schema, struct, 0L);
        var result = transform.apply(record);

        var expectedHeaders = new ConnectHeaders();
        expectedHeaders.add("header_from_field_1", 32, Schema.INT32_SCHEMA);
        expectedHeaders.add("header_from_field_2", 64.0d, Schema.OPTIONAL_FLOAT64_SCHEMA);

        // struct should be the same
        assertEquals(struct, result.value());
        assertEquals(expectedHeaders, result.headers());
    }

    @Test
    public void headerFromNestedFields() {
        var transform = new HeaderFromField.Key<SinkRecord>();
        transform.configure(Map.of(
                "mappings", "plain:header_from_plain_field,struct.inner_struct_a:header_from_nested_struct_field"
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

        var expectedHeaders = new ConnectHeaders();
        expectedHeaders.add("header_from_nested_struct_field", false, Schema.BOOLEAN_SCHEMA);
        expectedHeaders.add("header_from_plain_field", 321, Schema.INT32_SCHEMA);

        // struct should be the same
        assertEquals(struct, result.key());
        assertEquals(expectedHeaders, result.headers());
    }
}
