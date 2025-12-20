package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BytesToBase64Test {
    private byte[] string2bytes(String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void encodeBytesToBase64String() {
        var transform = new BytesToBase64<SinkRecord>();
        transform.configure(Map.of(
                "key.fields", "field_1",
                "value.fields", "field_2"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.BYTES_SCHEMA)
                .field("field_2", Schema.BYTES_SCHEMA)
                .build();

        var keyStruct = new Struct(schema).put("field_1", string2bytes("field_1_value")).put("field_2", string2bytes("field_2_value"));
        var valueStruct = new Struct(schema).put("field_1", string2bytes("field_1_value")).put("field_2", string2bytes("field_2_value"));

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var resultKeyStruct = requireStruct(result.key(), "test");
        var resultValueStruct = requireStruct(result.value(), "test");

        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.BYTES_SCHEMA)
                .build();
        var expectedValueSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.BYTES_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .build();

        var expectedKeyStruct = new Struct(expectedKeySchema).put("field_1", "ZmllbGRfMV92YWx1ZQ==").put("field_2", string2bytes("field_2_value"));
        var expectedValueStruct = new Struct(expectedValueSchema).put("field_1", string2bytes("field_1_value")).put("field_2", "ZmllbGRfMl92YWx1ZQ==");

        assertEquals(expectedKeySchema, result.keySchema());
        assertEquals(expectedValueSchema, result.valueSchema());

        assertEquals(expectedKeyStruct, resultKeyStruct);
        assertEquals(expectedValueStruct, resultValueStruct);
    }

    @Test
    public void encodeAllFieldsToBase64() {
        var transform = new BytesToBase64<SinkRecord>();
        transform.configure(Map.of(
                "key.fields", "*",
                "value.fields", "*"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("field_1", Schema.BYTES_SCHEMA)
                .field("field_2", Schema.BYTES_SCHEMA)
                .build();

        var keyStruct = new Struct(schema).put("field_1", string2bytes("field_1_value")).put("field_2", string2bytes("field_2_value"));
        var valueStruct = new Struct(schema).put("field_1", string2bytes("field_1_value")).put("field_2", string2bytes("field_2_value"));

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var resultKeyStruct = requireStruct(result.key(), "test");
        var resultValueStruct = requireStruct(result.value(), "test");

        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .build();
        var expectedValueSchema = SchemaBuilder
                .struct()
                .field("field_1", Schema.STRING_SCHEMA)
                .field("field_2", Schema.STRING_SCHEMA)
                .build();

        var expectedKeyStruct = new Struct(expectedKeySchema).put("field_1", "ZmllbGRfMV92YWx1ZQ==").put("field_2", "ZmllbGRfMl92YWx1ZQ==");
        var expectedValueStruct = new Struct(expectedValueSchema).put("field_1", "ZmllbGRfMV92YWx1ZQ==").put("field_2", "ZmllbGRfMl92YWx1ZQ==");

        assertEquals(expectedKeySchema, result.keySchema());
        assertEquals(expectedValueSchema, result.valueSchema());

        assertEquals(expectedKeyStruct, resultKeyStruct);
        assertEquals(expectedValueStruct, resultValueStruct);
    }

    @Test
    public void encodeNestedFieldsToBase64() {
        var transform = new BytesToBase64<SinkRecord>();
        transform.configure(Map.of(
                "key.fields", "nested.nested_field_1",
                "value.fields", "nested.nested_level_2"
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

        var keyStruct = new Struct(schema)
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
        var valueStruct = new Struct(schema)
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

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var resultKeyStruct = requireStruct(result.key(), "test");
        var resultValueStruct = requireStruct(result.value(), "test");

        var expectedKeySchema = SchemaBuilder
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

        var expectedValueSchema = SchemaBuilder
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
                                                .field("nested_level_2_field_1", Schema.STRING_SCHEMA)
                                                .field("nested_level_2_field_2", Schema.STRING_SCHEMA)
                                                .optional()
                                                .build()
                                )
                                .optional()
                                .build()
                )
                .build();

        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("field_1", string2bytes("field_1_value"))
                .put(
                        "nested",
                        new Struct(expectedKeySchema.field("nested").schema())
                                .put("nested_field_1", "bmVzdGVkX2ZpZWxkXzFfdmFsdWU=")
                                .put(
                                        "nested_level_2",
                                        new Struct(expectedKeySchema.field("nested").schema().field("nested_level_2").schema())
                                                .put("nested_level_2_field_1", string2bytes("nested_level_2_field_1_value"))
                                                .put("nested_level_2_field_2", string2bytes("nested_level_2_field_2_value"))
                                )
                );
        var expectedValueStruct = new Struct(expectedValueSchema)
                .put("field_1", string2bytes("field_1_value"))
                .put(
                        "nested",
                        new Struct(expectedValueSchema.field("nested").schema())
                                .put("nested_field_1", string2bytes("nested_field_1_value"))
                                .put(
                                        "nested_level_2",
                                        new Struct(expectedValueSchema.field("nested").schema().field("nested_level_2").schema())
                                                .put("nested_level_2_field_1", "bmVzdGVkX2xldmVsXzJfZmllbGRfMV92YWx1ZQ==")
                                                .put("nested_level_2_field_2", "bmVzdGVkX2xldmVsXzJfZmllbGRfMl92YWx1ZQ==")
                                )
                );

        assertEquals(expectedKeySchema, result.keySchema());
        assertEquals(expectedValueSchema, result.valueSchema());

        assertEquals(expectedKeyStruct, resultKeyStruct);
        assertEquals(expectedValueStruct, resultValueStruct);
    }
}
