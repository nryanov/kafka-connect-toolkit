package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class ReplaceFieldNameTest {
    @Test
    public void excludeField() {
        var transform = new ReplaceFieldName<SinkRecord>();
        transform.configure(Map.of(
                "key.exclude", "b",
                "value.exclude", "c"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var keyStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var valueStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultValueStruct = requireStruct(result.value(), "test");
        var resultKeyStruct = requireStruct(result.key(), "test");

        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("a", "a_field_value")
                .put("c", "c_field_value");

        var expectedValueSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .build();

        var expectedValueStruct = new Struct(expectedValueSchema)
                .put("a", "a_field_value")
                .put("b", 1);

        assertEquals(expectedKeySchema, resultKeyStruct.schema());
        assertEquals(expectedKeyStruct, resultKeyStruct);

        assertEquals(expectedValueSchema, resultValueStruct.schema());
        assertEquals(expectedValueStruct, resultValueStruct);
    }

    @Test
    public void includeField() {
        var transform = new ReplaceFieldName<SinkRecord>();
        transform.configure(Map.of(
                "key.include", "a",
                "value.include", "b"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var keyStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var valueStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultValueStruct = requireStruct(result.value(), "test");
        var resultKeyStruct = requireStruct(result.key(), "test");

        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .build();

        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("a", "a_field_value");

        var expectedValueSchema = SchemaBuilder
                .struct()
                .field("b", Schema.INT32_SCHEMA)
                .build();

        var expectedValueStruct = new Struct(expectedValueSchema)
                .put("b", 1);

        assertEquals(expectedKeySchema, resultKeyStruct.schema());
        assertEquals(expectedKeyStruct, resultKeyStruct);

        assertEquals(expectedValueSchema, resultValueStruct.schema());
        assertEquals(expectedValueStruct, resultValueStruct);
    }

    @Test
    public void renameField() {
        var transform = new ReplaceFieldName<SinkRecord>();
        transform.configure(Map.of(
                "key.replace", "a:renamed_a",
                "value.replace", "b:renamed_b"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var keyStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var valueStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultValueStruct = requireStruct(result.value(), "test");
        var resultKeyStruct = requireStruct(result.key(), "test");

        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("renamed_a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("renamed_a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var expectedValueSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("renamed_b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var expectedValueStruct = new Struct(expectedValueSchema)
                .put("a", "a_field_value")
                .put("renamed_b", 1)
                .put("c", "c_field_value");

        assertEquals(expectedKeySchema, resultKeyStruct.schema());
        assertEquals(expectedKeyStruct, resultKeyStruct);

        assertEquals(expectedValueSchema, resultValueStruct.schema());
        assertEquals(expectedValueStruct, resultValueStruct);
    }

//    @Test
//    public void excludeFieldFromKey() {
//        var transform = new ReplaceFieldName<SinkRecord>();
//        transform.configure(Map.of(
//                "key.exclude", "b"
//        ));
//
//        var keyStruct = DefaultRecords.createNestedKeyStruct();
//        var valueStruct = DefaultRecords.createNestedValueStruct();
//
//        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);
//
//        var result = transform.apply(record);
//        var resultKeyStruct = requireStruct(result.key(), "test");
//
//        assertNotNull(result.keySchema().field("a"));
//        assertNull(result.keySchema().field("b"));
//        assertNotNull(result.keySchema().field("c"));
//
//        var innerKeyArraySchema = keyStruct.schema().field("c").schema().field("inner_c").schema().valueSchema();
//        var expectedKeyStruct = new Struct(keyStruct.schema());
//        expectedKeyStruct.put("a", "a_field_value");
//        expectedKeyStruct.put("c", new Struct(keyStruct.schema().field("c").schema())
//                .put("inner_a", "inner_a_value")
//                .put("inner_b", "inner_b_value")
//                .put("inner_c", List.of(
//                        new Struct(innerKeyArraySchema)
//                                .put("inner_inner_a", "inner_inner_a_array_value")
//                                .put("inner_inner_b", 3)
//                ))
//
//        );
//
//        assertEquals(expectedKeyStruct.get("a"), resultKeyStruct.get("a"));
//        assertThrows(DataException.class, () -> resultKeyStruct.get("b"));
//        assertEquals(expectedKeyStruct.getStruct("c"), resultKeyStruct.getStruct("c"));
//    }
//
//    @Test
//    public void includeSubsetOfFieldsFromKey() {
//        var transform = new ReplaceFieldName<SinkRecord>();
//        transform.configure(Map.of(
//                "key.include", "c"
//        ));
//
//        var keyStruct = DefaultRecords.createNestedKeyStruct();
//        var valueStruct = DefaultRecords.createNestedValueStruct();
//
//        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);
//
//        var result = transform.apply(record);
//        var resultKeyStruct = requireStruct(result.key(), "test");
//
//        var innerKeyArraySchema = keyStruct.schema().field("c").schema().field("inner_c").schema().valueSchema();
//        var expectedKeyStruct = new Struct(keyStruct.schema());
//        expectedKeyStruct.put("c", new Struct(keyStruct.schema().field("c").schema())
//                .put("inner_a", "inner_a_value")
//                .put("inner_b", "inner_b_value")
//                .put("inner_c", List.of(
//                        new Struct(innerKeyArraySchema)
//                                .put("inner_inner_a", "inner_inner_a_array_value")
//                                .put("inner_inner_b", 3)
//                ))
//
//        );
//
//        assertThrows(DataException.class, () -> resultKeyStruct.get("a"));
//        assertThrows(DataException.class, () -> resultKeyStruct.get("b"));
//        assertEquals(expectedKeyStruct.getStruct("c"), resultKeyStruct.getStruct("c"));
//    }
//
//    @Test
//    public void includeSubsetOfNestedFieldsFromKey() {
//        var transform = new ReplaceFieldName<SinkRecord>();
//        transform.configure(Map.of(
//                "key.include", "c.inner_a"
//        ));
//
//        var keyStruct = DefaultRecords.createNestedKeyStruct();
//        var valueStruct = DefaultRecords.createNestedValueStruct();
//
//        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);
//
//        var result = transform.apply(record);
//        var resultKeyStruct = requireStruct(result.key(), "test");
//
//        var expectedKeySchema = SchemaBuilder.struct().field("c", SchemaBuilder.struct().field("inner_a", Schema.STRING_SCHEMA).build()).build();
//        var expectedKeyStruct = new Struct(expectedKeySchema);
//        expectedKeyStruct.put("c", new Struct(expectedKeySchema.schema().field("c").schema()).put("inner_a", "inner_a_value"));
//
//        assertEquals(expectedKeySchema, resultKeyStruct.schema());
//        assertEquals(expectedKeyStruct, resultKeyStruct);
//    }
//
//    @Test
//    public void includeSubsetOfNestedArrayFieldsFromKey() {
//        var transform = new ReplaceFieldName<SinkRecord>();
//        transform.configure(Map.of(
//                "key.include", "c.inner_c.inner_inner_a"
//        ));
//
//        var keyStruct = DefaultRecords.createNestedKeyStruct();
//        var valueStruct = DefaultRecords.createNestedValueStruct();
//
//        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);
//
//        var result = transform.apply(record);
//        var resultKeyStruct = requireStruct(result.key(), "test");
//
//        var expectedInnerKeyArraySchema = SchemaBuilder.struct()
//                .field("inner_inner_a", Schema.STRING_SCHEMA)
//                .build();
//        var expectedKeySchema = SchemaBuilder
//                .struct()
//                .field("c", SchemaBuilder.struct()
//                        .field("inner_c", SchemaBuilder.array(expectedInnerKeyArraySchema).build())
//                        .build()
//                )
//                .build();
//
//        var expectedKeyStruct = new Struct(expectedKeySchema);
//        expectedKeyStruct.put("c", new Struct(expectedKeySchema.field("c").schema())
//                .put("inner_c", List.of(
//                        new Struct(expectedInnerKeyArraySchema)
//                                .put("inner_inner_a", "inner_inner_a_array_value")
//                ))
//
//        );
//
//        assertEquals(expectedKeySchema, resultKeyStruct.schema());
//        assertEquals(expectedKeyStruct, resultKeyStruct);
//    }
}
