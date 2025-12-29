package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BytesToStringTest {
    private byte[] string2bytes(String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void correctlyHandleNullPayload() {
        var transform = new BytesToString.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field_1"
        ));

        var schema = Schema.STRING_SCHEMA;

        var record = new SinkRecord("topic", 1, schema, null, null, null, 0L);
        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void correctlyHandleNullSchema() {
        var transform = new BytesToString.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field_1"
        ));

        var record = new SinkRecord("topic", 1, null, "value", null, null, 0L);
        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void decodeBytesToStringInKey() {
        var transform = new BytesToString.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field_1"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.BYTES_SCHEMA)
                .field("field_2", Schema.BYTES_SCHEMA)
                .build();

        var struct = new Struct(schema).put("field_1", string2bytes("field_1_value")).put("field_2", string2bytes("field_2_value"));

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.BYTES_SCHEMA)
                .build();

        var expectedStruct = new Struct(expectedSchema).put("field_1", "field_1_value").put("field_2", string2bytes("field_2_value"));

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void decodeBytesToStringInValue() {
        var transform = new BytesToString.Value<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field_1"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.BYTES_SCHEMA)
                .field("field_2", Schema.BYTES_SCHEMA)
                .build();

        var struct = new Struct(schema).put("field_1", string2bytes("field_1_value")).put("field_2", string2bytes("field_2_value"));

        var record = new SinkRecord("topic", 1, null, null, struct.schema(), struct, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.value(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.BYTES_SCHEMA)
                .build();

        var expectedStruct = new Struct(expectedSchema).put("field_1", "field_1_value").put("field_2", string2bytes("field_2_value"));

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void decodeBytesToStringInAllFieldsInKey() {
        var transform = new BytesToString.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "*"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.BYTES_SCHEMA)
                .field("field_2", Schema.BYTES_SCHEMA)
                .build();

        var struct = new Struct(schema).put("field_1", string2bytes("field_1_value")).put("field_2", string2bytes("field_2_value"));

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);
        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .build();

        var expectedStruct = new Struct(expectedSchema).put("field_1", "field_1_value").put("field_2", "field_2_value");

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void decodeBytesToStringInNestedFields() {
        var transform = new BytesToString.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "nested.nested_field_1"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.BYTES_SCHEMA)
                .field(
                        "nested",
                        SchemaBuilder
                                .struct()
                                .field("nested_field_1", Schema.BYTES_SCHEMA)
                                .field(
                                        "nested_level_2",
                                        SchemaBuilder
                                                .struct()
                                                .field("nested_level_2_field_1", Schema.BYTES_SCHEMA)
                                                .field("nested_level_2_field_2", Schema.BYTES_SCHEMA)
                                                .optional()
                                                .build()
                                )
                                .optional()
                                .build()
                )
                .build();

        var struct = new Struct(schema)
                .put("field_1", string2bytes("field_1_value"))
                .put(
                        "nested",
                        new Struct(schema.field("nested").schema())
                                .put("nested_field_1", string2bytes("nested_field_1_value"))
                                .put(
                                        "nested_level_2",
                                        new Struct(schema.field("nested").schema().field("nested_level_2").schema())
                                                .put("nested_level_2_field_1", string2bytes("nested_level_2_field_1_value"))
                                                .put("nested_level_2_field_2", string2bytes("nested_level_2_field_2_value"))
                                )
                );

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);
        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.BYTES_SCHEMA)
                .field(
                        "nested",
                        SchemaBuilder
                                .struct()
                                .field("nested_field_1", Schema.STRING_SCHEMA)
                                .field(
                                        "nested_level_2",
                                        SchemaBuilder
                                                .struct()
                                                .field("nested_level_2_field_1", Schema.BYTES_SCHEMA)
                                                .field("nested_level_2_field_2", Schema.BYTES_SCHEMA)
                                                .optional()
                                                .build()
                                )
                                .optional()
                                .build()
                )
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("field_1", string2bytes("field_1_value"))
                .put(
                        "nested",
                        new Struct(expectedSchema.field("nested").schema())
                                .put("nested_field_1", "nested_field_1_value")
                                .put(
                                        "nested_level_2",
                                        new Struct(expectedSchema.field("nested").schema().field("nested_level_2").schema())
                                                .put("nested_level_2_field_1", string2bytes("nested_level_2_field_1_value"))
                                                .put("nested_level_2_field_2", string2bytes("nested_level_2_field_2_value"))
                                )
                );

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }
}
