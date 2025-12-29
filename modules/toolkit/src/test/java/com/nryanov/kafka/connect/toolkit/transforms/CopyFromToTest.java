package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CopyFromToTest {
    @Test
    public void correctlyHandleNullPayload() {
        var transform = new CopyFromTo.KeyToValue<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field_1"
        ));

        var schema = Schema.STRING_SCHEMA;

        var record = new SinkRecord("topic", 1, schema, null, null, null, 0L);
        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void correctlyHandleNullSchema() {
        var transform = new CopyFromTo.KeyToValue<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field_1"
        ));

        var record = new SinkRecord("topic", 1, null, "value", null, null, 0L);
        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void addFieldsToValueFromKey() {
        var transform = new CopyFromTo.KeyToValue<SinkRecord>();
        transform.configure(Map.of(
                "fields", "b:copied_b,c:copied_c"
        ));

        var keySchema = SchemaBuilder
                .struct()
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var keyStruct = new Struct(keySchema)
                .put("b", 1)
                .put("c", "c_field_value");

        var valueSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .build();

        var valueStruct = new Struct(valueSchema)
                .put("a", "a_field_value");

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var expectedSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("copied_b", Schema.INT32_SCHEMA)
                .field("copied_c", Schema.STRING_SCHEMA)
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("a", "a_field_value")
                .put("copied_b", 1)
                .put("copied_c", "c_field_value");

        assertEquals(expectedSchema, result.valueSchema());
        assertEquals(expectedStruct, result.value());
    }

    @Test
    public void copyAllFieldsToValueFromKey() {
        var transform = new CopyFromTo.KeyToValue<SinkRecord>();
        transform.configure(Map.of(
                "fields", "*"
        ));

        var keySchema = SchemaBuilder
                .struct()
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var keyStruct = new Struct(keySchema)
                .put("b", 1)
                .put("c", "c_field_value");

        var valueSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .build();

        var valueStruct = new Struct(valueSchema)
                .put("a", "a_field_value");

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var expectedSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b_copy", Schema.INT32_SCHEMA)
                .field("c_copy", Schema.STRING_SCHEMA)
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("a", "a_field_value")
                .put("b_copy", 1)
                .put("c_copy", "c_field_value");

        assertEquals(expectedSchema, result.valueSchema());
        assertEquals(expectedStruct, result.value());
    }

    @Test
    public void copyNestedFieldsFromKeyToValue() {
        var transform = new CopyFromTo.KeyToValue<SinkRecord>();
        // only single field from array and all fields in struct
        transform.configure(Map.of(
                "fields", "array.inner_array_b:single_array_field,struct:all_struct"
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

        var keySchema = SchemaBuilder
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
        var keyStruct = new Struct(keySchema)
                .put("plain", "plain_value")
                .put("array", List.of(nestedArrayStruct1, nestedArrayStruct2))
                .put("struct", nestedStruct);

        var valueSchema = SchemaBuilder
                .struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();

        var valueStruct = new Struct(valueSchema)
                .put("id", "some id");

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);
        var result = transform.apply(record);

        var expectedNestedArrayStructSchema = SchemaBuilder
                .struct()
                .field("single_array_field", Schema.STRING_SCHEMA)
                .build();
        var expectedNestedStructSchema = SchemaBuilder
                .struct()
                .field("inner_struct_a_copy", Schema.OPTIONAL_INT64_SCHEMA)
                .field("inner_struct_b_copy", Schema.OPTIONAL_STRING_SCHEMA)
                .field("inner_struct_c_copy", Schema.INT32_SCHEMA)
                .optional()
                .build();
        var expectedSchema = SchemaBuilder
                .struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("array_copy", SchemaBuilder.array(expectedNestedArrayStructSchema).build())
                .field("all_struct", expectedNestedStructSchema)
                .build();

        var expectedNestedArrayStruct1 = new Struct(expectedNestedArrayStructSchema)
                .put("single_array_field", "inner_array_b_value_1");
        var expectedNestedArrayStruct2 = new Struct(expectedNestedArrayStructSchema)
                .put("single_array_field", "inner_array_b_value_2");
        var expectedNestedStruct = new Struct(expectedNestedStructSchema)
                .put("inner_struct_a_copy", 64L)
                .put("inner_struct_b_copy", "inner_struct_b_value")
                .put("inner_struct_c_copy", 32);
        var expectedStruct = new Struct(expectedSchema)
                .put("id", "some id")
                .put("array_copy", List.of(expectedNestedArrayStruct1, expectedNestedArrayStruct2))
                .put("all_struct", expectedNestedStruct);

        assertEquals(expectedSchema, result.valueSchema());
        assertEquals(expectedStruct, result.value());
    }

    @Test
    public void copyNestedFieldsWithCustomDefaultSuffixFromKeyToValue() {
        var transform = new CopyFromTo.KeyToValue<SinkRecord>();
        // only single field from array and all fields in struct
        transform.configure(Map.of(
                "fields", "array.inner_array_b:single_array_field,struct:all_struct",
                "suffix", "_custom"
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
        var keySchema = SchemaBuilder
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
        var keyStruct = new Struct(keySchema)
                .put("plain", "plain_value")
                .put("array", List.of(nestedArrayStruct1, nestedArrayStruct2))
                .put("struct", nestedStruct);

        var valueSchema = SchemaBuilder
                .struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();

        var valueStruct = new Struct(valueSchema)
                .put("id", "some id");

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);
        var result = transform.apply(record);

        var expectedNestedArrayStructSchema = SchemaBuilder
                .struct()
                .field("single_array_field", Schema.STRING_SCHEMA)
                .build();
        var expectedNestedStructSchema = SchemaBuilder
                .struct()
                .field("inner_struct_a_custom", Schema.OPTIONAL_INT64_SCHEMA)
                .field("inner_struct_b_custom", Schema.OPTIONAL_STRING_SCHEMA)
                .field("inner_struct_c_custom", Schema.INT32_SCHEMA)
                .optional()
                .build();
        var expectedSchema = SchemaBuilder
                .struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("array_custom", SchemaBuilder.array(expectedNestedArrayStructSchema).build())
                .field("all_struct", expectedNestedStructSchema)
                .build();

        var expectedNestedArrayStruct1 = new Struct(expectedNestedArrayStructSchema)
                .put("single_array_field", "inner_array_b_value_1");
        var expectedNestedArrayStruct2 = new Struct(expectedNestedArrayStructSchema)
                .put("single_array_field", "inner_array_b_value_2");
        var expectedNestedStruct = new Struct(expectedNestedStructSchema)
                .put("inner_struct_a_custom", 64L)
                .put("inner_struct_b_custom", "inner_struct_b_value")
                .put("inner_struct_c_custom", 32);
        var expectedStruct = new Struct(expectedSchema)
                .put("id", "some id")
                .put("array_custom", List.of(expectedNestedArrayStruct1, expectedNestedArrayStruct2))
                .put("all_struct", expectedNestedStruct);

        assertEquals(expectedSchema, result.valueSchema());
        assertEquals(expectedStruct, result.value());
    }

    @Test
    public void addFieldsToKeyFromValue() {
        var transform = new CopyFromTo.ValueToKey<SinkRecord>();
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

    @Test
    public void copyAllFields() {
        var transform = new CopyFromTo.ValueToKey<SinkRecord>();
        transform.configure(Map.of(
                "fields", "*"
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
                .field("b_copy", Schema.INT32_SCHEMA)
                .field("c_copy", Schema.STRING_SCHEMA)
                .build();

        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("a", "a_field_value")
                .put("b_copy", 1)
                .put("c_copy", "c_field_value");

        assertEquals(expectedKeySchema, result.keySchema());
        assertEquals(expectedKeyStruct, result.key());
    }

    @Test
    public void copyNestedFieldsFromValueToKey() {
        var transform = new CopyFromTo.ValueToKey<SinkRecord>();
        // only single field from array and all fields in struct
        transform.configure(Map.of(
                "fields", "array.inner_array_b:single_array_field,struct:all_struct"
        ));

        var keySchema = SchemaBuilder
                .struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();

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
        var valueSchema = SchemaBuilder
                .struct()
                .field("plain", Schema.STRING_SCHEMA)
                .field("array", SchemaBuilder.array(nestedArrayStructSchema).build())
                .field("struct", nestedStructSchema)
                .build();

        var keyStruct = new Struct(keySchema)
                .put("id", "some id");

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
        var valueStruct = new Struct(valueSchema)
                .put("plain", "plain_value")
                .put("array", List.of(nestedArrayStruct1, nestedArrayStruct2))
                .put("struct", nestedStruct);

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);
        var result = transform.apply(record);

        var expectedNestedArrayStructSchema = SchemaBuilder
                .struct()
                .field("single_array_field", Schema.STRING_SCHEMA)
                .build();
        var expectedNestedStructSchema = SchemaBuilder
                .struct()
                .field("inner_struct_a_copy", Schema.OPTIONAL_INT64_SCHEMA)
                .field("inner_struct_b_copy", Schema.OPTIONAL_STRING_SCHEMA)
                .field("inner_struct_c_copy", Schema.INT32_SCHEMA)
                .optional()
                .build();
        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("array_copy", SchemaBuilder.array(expectedNestedArrayStructSchema).build())
                .field("all_struct", expectedNestedStructSchema)
                .build();

        var expectedNestedArrayStruct1 = new Struct(expectedNestedArrayStructSchema)
                .put("single_array_field", "inner_array_b_value_1");
        var expectedNestedArrayStruct2 = new Struct(expectedNestedArrayStructSchema)
                .put("single_array_field", "inner_array_b_value_2");
        var expectedNestedStruct = new Struct(expectedNestedStructSchema)
                .put("inner_struct_a_copy", 64L)
                .put("inner_struct_b_copy", "inner_struct_b_value")
                .put("inner_struct_c_copy", 32);
        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("id", "some id")
                .put("array_copy", List.of(expectedNestedArrayStruct1, expectedNestedArrayStruct2))
                .put("all_struct", expectedNestedStruct);

        assertEquals(expectedKeySchema, result.keySchema());
        assertEquals(expectedKeyStruct, result.key());
    }

    @Test
    public void copyNestedFieldsWithCustomDefaultSuffixFromValueToKey() {
        var transform = new CopyFromTo.ValueToKey<SinkRecord>();
        // only single field from array and all fields in struct
        transform.configure(Map.of(
                "fields", "array.inner_array_b:single_array_field,struct:all_struct",
                "suffix", "_custom"
        ));

        var keySchema = SchemaBuilder
                .struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();

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
        var valueSchema = SchemaBuilder
                .struct()
                .field("plain", Schema.STRING_SCHEMA)
                .field("array", SchemaBuilder.array(nestedArrayStructSchema).build())
                .field("struct", nestedStructSchema)
                .build();

        var keyStruct = new Struct(keySchema)
                .put("id", "some id");

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
        var valueStruct = new Struct(valueSchema)
                .put("plain", "plain_value")
                .put("array", List.of(nestedArrayStruct1, nestedArrayStruct2))
                .put("struct", nestedStruct);

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);
        var result = transform.apply(record);

        var expectedNestedArrayStructSchema = SchemaBuilder
                .struct()
                .field("single_array_field", Schema.STRING_SCHEMA)
                .build();
        var expectedNestedStructSchema = SchemaBuilder
                .struct()
                .field("inner_struct_a_custom", Schema.OPTIONAL_INT64_SCHEMA)
                .field("inner_struct_b_custom", Schema.OPTIONAL_STRING_SCHEMA)
                .field("inner_struct_c_custom", Schema.INT32_SCHEMA)
                .optional()
                .build();
        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("array_custom", SchemaBuilder.array(expectedNestedArrayStructSchema).build())
                .field("all_struct", expectedNestedStructSchema)
                .build();

        var expectedNestedArrayStruct1 = new Struct(expectedNestedArrayStructSchema)
                .put("single_array_field", "inner_array_b_value_1");
        var expectedNestedArrayStruct2 = new Struct(expectedNestedArrayStructSchema)
                .put("single_array_field", "inner_array_b_value_2");
        var expectedNestedStruct = new Struct(expectedNestedStructSchema)
                .put("inner_struct_a_custom", 64L)
                .put("inner_struct_b_custom", "inner_struct_b_value")
                .put("inner_struct_c_custom", 32);
        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("id", "some id")
                .put("array_custom", List.of(expectedNestedArrayStruct1, expectedNestedArrayStruct2))
                .put("all_struct", expectedNestedStruct);

        assertEquals(expectedKeySchema, result.keySchema());
        assertEquals(expectedKeyStruct, result.key());
    }
}
