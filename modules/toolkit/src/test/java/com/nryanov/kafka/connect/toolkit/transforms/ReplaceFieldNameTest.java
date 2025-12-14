package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
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

    @Test
    public void excludeNestedField() {
        var transform = new ReplaceFieldName<SinkRecord>();
        transform.configure(Map.of(
                "key.exclude", "b.inner_a",
                "value.exclude", "b.inner_b"
        ));

        var nestedSchema = SchemaBuilder
                .struct()
                .field("inner_a", Schema.STRING_SCHEMA)
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", nestedSchema)
                .build();

        var nestedKeyStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var keyStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", nestedKeyStruct);

        var nestedValueStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var valueStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", nestedValueStruct);

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultValueStruct = requireStruct(result.value(), "test");
        var resultKeyStruct = requireStruct(result.key(), "test");

        var expectedNestedKeySchema = SchemaBuilder
                .struct()
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", expectedNestedKeySchema)
                .build();

        var expectedNestedValueSchema = SchemaBuilder
                .struct()
                .field("inner_a", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedValueSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", expectedNestedValueSchema)
                .build();

        var expectedNestedKeyStruct = new Struct(expectedNestedKeySchema)
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("a", "a_field_value")
                .put("b", expectedNestedKeyStruct);

        var expectedNestedValueStruct = new Struct(expectedNestedValueSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_c", "inner_c_value");
        var expectedValueStruct = new Struct(expectedValueSchema)
                .put("a", "a_field_value")
                .put("b", expectedNestedValueStruct);

        assertEquals(expectedKeySchema, resultKeyStruct.schema());
        assertEquals(expectedKeyStruct, resultKeyStruct);

        assertEquals(expectedValueSchema, resultValueStruct.schema());
        assertEquals(expectedValueStruct, resultValueStruct);
    }

    @Test
    public void includeNestedField() {
        var transform = new ReplaceFieldName<SinkRecord>();
        transform.configure(Map.of(
                "key.include", "b.inner_b",
                "value.include", "b.inner_c"
        ));

        var nestedSchema = SchemaBuilder
                .struct()
                .field("inner_a", Schema.STRING_SCHEMA)
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", nestedSchema)
                .build();

        var nestedKeyStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var keyStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", nestedKeyStruct);

        var nestedValueStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var valueStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", nestedValueStruct);

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultValueStruct = requireStruct(result.value(), "test");
        var resultKeyStruct = requireStruct(result.key(), "test");

        var expectedNestedKeySchema = SchemaBuilder
                .struct()
                .field("inner_b", Schema.STRING_SCHEMA)
                .build();
        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("b", expectedNestedKeySchema)
                .build();

        var expectedNestedValueSchema = SchemaBuilder
                .struct()
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedValueSchema = SchemaBuilder
                .struct()
                .field("b", expectedNestedValueSchema)
                .build();

        var expectedNestedKeyStruct = new Struct(expectedNestedKeySchema)
                .put("inner_b", "inner_b_value");
        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("b", expectedNestedKeyStruct);

        var expectedNestedValueStruct = new Struct(expectedNestedValueSchema)
                .put("inner_c", "inner_c_value");
        var expectedValueStruct = new Struct(expectedValueSchema)
                .put("b", expectedNestedValueStruct);

        assertEquals(expectedKeySchema, resultKeyStruct.schema());
        assertEquals(expectedKeyStruct, resultKeyStruct);

        assertEquals(expectedValueSchema, resultValueStruct.schema());
        assertEquals(expectedValueStruct, resultValueStruct);
    }

    @Test
    public void renameNestedField() {
        var transform = new ReplaceFieldName<SinkRecord>();
        transform.configure(Map.of(
                "key.replace", "b.inner_a:inner_a_renamed",
                "value.replace", "b.inner_b:inner_b_renamed"
        ));

        var nestedSchema = SchemaBuilder
                .struct()
                .field("inner_a", Schema.STRING_SCHEMA)
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", nestedSchema)
                .build();

        var nestedKeyStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var keyStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", nestedKeyStruct);

        var nestedValueStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var valueStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", nestedValueStruct);

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultValueStruct = requireStruct(result.value(), "test");
        var resultKeyStruct = requireStruct(result.key(), "test");

        var expectedNestedKeySchema = SchemaBuilder
                .struct()
                .field("inner_a_renamed", Schema.STRING_SCHEMA)
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", expectedNestedKeySchema)
                .build();

        var expectedNestedValueSchema = SchemaBuilder
                .struct()
                .field("inner_a", Schema.STRING_SCHEMA)
                .field("inner_b_renamed", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedValueSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", expectedNestedValueSchema)
                .build();

        var expectedNestedKeyStruct = new Struct(expectedNestedKeySchema)
                .put("inner_a_renamed", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("a", "a_field_value")
                .put("b", expectedNestedKeyStruct);

        var expectedNestedValueStruct = new Struct(expectedNestedValueSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b_renamed", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var expectedValueStruct = new Struct(expectedValueSchema)
                .put("a", "a_field_value")
                .put("b", expectedNestedValueStruct);

        assertEquals(expectedKeySchema, resultKeyStruct.schema());
        assertEquals(expectedKeyStruct, resultKeyStruct);

        assertEquals(expectedValueSchema, resultValueStruct.schema());
        assertEquals(expectedValueStruct, resultValueStruct);
    }

    @Test
    public void excludeNestedArrayField() {
        var transform = new ReplaceFieldName<SinkRecord>();
        transform.configure(Map.of(
                "key.exclude", "b.inner_a",
                "value.exclude", "b.inner_b"
        ));

        var nestedSchema = SchemaBuilder
                .struct()
                .field("inner_a", Schema.STRING_SCHEMA)
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", SchemaBuilder.array(nestedSchema).build())
                .build();

        var nestedKeyStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var keyStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", List.of(nestedKeyStruct));

        var nestedValueStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var valueStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", List.of(nestedValueStruct));

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultValueStruct = requireStruct(result.value(), "test");
        var resultKeyStruct = requireStruct(result.key(), "test");

        var expectedNestedKeySchema = SchemaBuilder
                .struct()
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", SchemaBuilder.array(expectedNestedKeySchema).build())
                .build();

        var expectedNestedValueSchema = SchemaBuilder
                .struct()
                .field("inner_a", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedValueSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", SchemaBuilder.array(expectedNestedValueSchema).build())
                .build();

        var expectedNestedKeyStruct = new Struct(expectedNestedKeySchema)
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("a", "a_field_value")
                .put("b", List.of(expectedNestedKeyStruct));

        var expectedNestedValueStruct = new Struct(expectedNestedValueSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_c", "inner_c_value");
        var expectedValueStruct = new Struct(expectedValueSchema)
                .put("a", "a_field_value")
                .put("b", List.of(expectedNestedValueStruct));

        assertEquals(expectedKeySchema, resultKeyStruct.schema());
        assertEquals(expectedKeyStruct, resultKeyStruct);

        assertEquals(expectedValueSchema, resultValueStruct.schema());
        assertEquals(expectedValueStruct, resultValueStruct);
    }

    @Test
    public void includeNestedArrayField() {
        var transform = new ReplaceFieldName<SinkRecord>();
        transform.configure(Map.of(
                "key.include", "b.inner_b",
                "value.include", "b.inner_c"
        ));

        var nestedSchema = SchemaBuilder
                .struct()
                .field("inner_a", Schema.STRING_SCHEMA)
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", SchemaBuilder.array(nestedSchema).build())
                .build();

        var nestedKeyStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var keyStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", List.of(nestedKeyStruct));

        var nestedValueStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var valueStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", List.of(nestedValueStruct));

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultValueStruct = requireStruct(result.value(), "test");
        var resultKeyStruct = requireStruct(result.key(), "test");

        var expectedNestedKeySchema = SchemaBuilder
                .struct()
                .field("inner_b", Schema.STRING_SCHEMA)
                .build();
        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("b", SchemaBuilder.array(expectedNestedKeySchema).build())
                .build();

        var expectedNestedValueSchema = SchemaBuilder
                .struct()
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedValueSchema = SchemaBuilder
                .struct()
                .field("b", SchemaBuilder.array(expectedNestedValueSchema).build())
                .build();

        var expectedNestedKeyStruct = new Struct(expectedNestedKeySchema)
                .put("inner_b", "inner_b_value");
        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("b", List.of(expectedNestedKeyStruct));

        var expectedNestedValueStruct = new Struct(expectedNestedValueSchema)
                .put("inner_c", "inner_c_value");
        var expectedValueStruct = new Struct(expectedValueSchema)
                .put("b", List.of(expectedNestedValueStruct));

        assertEquals(expectedKeySchema, resultKeyStruct.schema());
        assertEquals(expectedKeyStruct, resultKeyStruct);

        assertEquals(expectedValueSchema, resultValueStruct.schema());
        assertEquals(expectedValueStruct, resultValueStruct);
    }

    @Test
    public void renameNestedArrayField() {
        var transform = new ReplaceFieldName<SinkRecord>();
        transform.configure(Map.of(
                "key.replace", "b.inner_a:inner_a_renamed",
                "value.replace", "b.inner_b:inner_b_renamed"
        ));

        var nestedSchema = SchemaBuilder
                .struct()
                .field("inner_a", Schema.STRING_SCHEMA)
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", SchemaBuilder.array(nestedSchema).build())
                .build();

        var nestedKeyStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var keyStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", List.of(nestedKeyStruct));

        var nestedValueStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var valueStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", List.of(nestedValueStruct));

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultValueStruct = requireStruct(result.value(), "test");
        var resultKeyStruct = requireStruct(result.key(), "test");

        var expectedNestedKeySchema = SchemaBuilder
                .struct()
                .field("inner_a_renamed", Schema.STRING_SCHEMA)
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedKeySchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", SchemaBuilder.array(expectedNestedKeySchema).build())
                .build();

        var expectedNestedValueSchema = SchemaBuilder
                .struct()
                .field("inner_a", Schema.STRING_SCHEMA)
                .field("inner_b_renamed", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedValueSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", SchemaBuilder.array(expectedNestedValueSchema).build())
                .build();

        var expectedNestedKeyStruct = new Struct(expectedNestedKeySchema)
                .put("inner_a_renamed", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var expectedKeyStruct = new Struct(expectedKeySchema)
                .put("a", "a_field_value")
                .put("b", List.of(expectedNestedKeyStruct));

        var expectedNestedValueStruct = new Struct(expectedNestedValueSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b_renamed", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var expectedValueStruct = new Struct(expectedValueSchema)
                .put("a", "a_field_value")
                .put("b", List.of(expectedNestedValueStruct));

        assertEquals(expectedKeySchema, resultKeyStruct.schema());
        assertEquals(expectedKeyStruct, resultKeyStruct);

        assertEquals(expectedValueSchema, resultValueStruct.schema());
        assertEquals(expectedValueStruct, resultValueStruct);
    }
}
