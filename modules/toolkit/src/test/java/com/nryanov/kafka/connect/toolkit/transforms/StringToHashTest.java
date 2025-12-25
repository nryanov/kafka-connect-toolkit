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

public class StringToHashTest {
    @Test
    public void mapStringValuesToHashInKey() {
        var transform = new StringToHash.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field_1:md5,field_2:sha1"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .build();
        var struct = new Struct(schema).put("field_1", "field_1_value").put("field_2", "field_2_value");

        var record = new SinkRecord("topic", 1, schema, struct, null, null, 0L);
        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("field_1", "78BFB84E6F4CB79B3C0E36DA36A96094")
                .put("field_2", "88B81DF19274AF63B67D3EBD14999B38D611921D");

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void mapStringValuesToHashInValue() {
        var transform = new StringToHash.Value<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field_1:md5,field_2:sha1"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .build();
        var struct = new Struct(schema).put("field_1", "field_1_value").put("field_2", "field_2_value");

        var record = new SinkRecord("topic", 1, null, null, schema, struct, 0L);
        var result = transform.apply(record);
        var resultStruct = requireStruct(result.value(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("field_1", "78BFB84E6F4CB79B3C0E36DA36A96094")
                .put("field_2", "88B81DF19274AF63B67D3EBD14999B38D611921D");

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void calculateHashFromNestedField() {
        var transform = new StringToHash.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "plain:md5,array.inner_array_b:md5,struct.inner_struct_b:sha1"
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

        var expectedArrayStruct1 = new Struct(nestedArrayStructSchema)
                .put("inner_array_a", null)
                .put("inner_array_b", "83E9ACA61D5A8A5D9C96EED1DD6AB4E6")
                .put("inner_array_c", "inner_array_c_value");
        var expectedArrayStruct2 = new Struct(nestedArrayStructSchema)
                .put("inner_array_a", "inner_array_a_value")
                .put("inner_array_b", "478254C778494CF2788ADE714005BE57")
                .put("inner_array_c", null);
        var expectedNestedStruct = new Struct(nestedStructSchema)
                .put("inner_struct_a", 64L)
                .put("inner_struct_b", "123D1DF7767A05F57768E2C1AFE37FB1055619D0")
                .put("inner_struct_c", 32);
        var expectedStruct = new Struct(schema)
                .put("plain", "A78634843A2BF517ED0D14A56BFC27C3")
                .put("array", List.of(expectedArrayStruct1, expectedArrayStruct2))
                .put("struct", expectedNestedStruct);

        assertEquals(expectedStruct, result.key());
    }
}
