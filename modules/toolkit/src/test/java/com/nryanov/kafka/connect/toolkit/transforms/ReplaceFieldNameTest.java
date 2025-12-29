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
    public void correctlyHandleNullPayload() {
        var transform = new ReplaceFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "exclude", "b,c"
        ));

        var schema = Schema.STRING_SCHEMA;

        var record = new SinkRecord("topic", 1, schema, null, null, null, 0L);
        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void correctlyHandleNullSchema() {
        var transform = new ReplaceFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "exclude", "b,c"
        ));

        var record = new SinkRecord("topic", 1, null, "value", null, null, 0L);
        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void excludeFieldFromKey() {
        var transform = new ReplaceFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "exclude", "b,c"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("a", "a_field_value");


        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void excludeFieldFromValue() {
        var transform = new ReplaceFieldName.Value<SinkRecord>();
        transform.configure(Map.of(
                "exclude", "b,c"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var record = new SinkRecord("topic", 1, null, null, struct.schema(), struct, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.value(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("a", "a_field_value");


        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void includeField() {
        var transform = new ReplaceFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "include", "a,b"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("a", "a_field_value")
                .put("b", 1);


        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void renameField() {
        var transform = new ReplaceFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "replace", "a:renamed_a"
        ));

        var schema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedSchema = SchemaBuilder
                .struct()
                .field("renamed_a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        var expectedStruct = new Struct(expectedSchema)
                .put("renamed_a", "a_field_value")
                .put("b", 1)
                .put("c", "c_field_value");

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void excludeNestedField() {
        var transform = new ReplaceFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "exclude", "b.inner_a"
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

        var nestedStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", nestedStruct);

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedNestedSchema = SchemaBuilder
                .struct()
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", expectedNestedSchema)
                .build();

        var expectedNestedStruct = new Struct(expectedNestedSchema)
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var expectedStruct = new Struct(expectedSchema)
                .put("a", "a_field_value")
                .put("b", expectedNestedStruct);

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void includeNestedField() {
        var transform = new ReplaceFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "include", "b.inner_b"
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

        var nestedStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", nestedStruct);

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedNestedSchema = SchemaBuilder
                .struct()
                .field("inner_b", Schema.STRING_SCHEMA)
                .build();
        var expectedSchema = SchemaBuilder
                .struct()
                .field("b", expectedNestedSchema)
                .build();

        var expectednestedStruct = new Struct(expectedNestedSchema)
                .put("inner_b", "inner_b_value");
        var expectedStruct = new Struct(expectedSchema)
                .put("b", expectednestedStruct);

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void renameNestedField() {
        var transform = new ReplaceFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "replace", "b.inner_a:inner_a_renamed"
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

        var nestedStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", nestedStruct);

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedNestedSchema = SchemaBuilder
                .struct()
                .field("inner_a_renamed", Schema.STRING_SCHEMA)
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", expectedNestedSchema)
                .build();

        var expectedNestedStruct = new Struct(expectedNestedSchema)
                .put("inner_a_renamed", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var expectedStruct = new Struct(expectedSchema)
                .put("a", "a_field_value")
                .put("b", expectedNestedStruct);

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void excludeNestedArrayField() {
        var transform = new ReplaceFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "exclude", "b.inner_a"
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

        var nestedStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", List.of(nestedStruct));

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedNestedSchema = SchemaBuilder
                .struct()
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", SchemaBuilder.array(expectedNestedSchema).build())
                .build();

        var expectednestedStruct = new Struct(expectedNestedSchema)
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var expectedStruct = new Struct(expectedSchema)
                .put("a", "a_field_value")
                .put("b", List.of(expectednestedStruct));

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void includeNestedArrayField() {
        var transform = new ReplaceFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "include", "b.inner_b"
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

        var nestedStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", List.of(nestedStruct));

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedNestedSchema = SchemaBuilder
                .struct()
                .field("inner_b", Schema.STRING_SCHEMA)
                .build();
        var expectedSchema = SchemaBuilder
                .struct()
                .field("b", SchemaBuilder.array(expectedNestedSchema).build())
                .build();

        var expectednestedStruct = new Struct(expectedNestedSchema)
                .put("inner_b", "inner_b_value");
        var expectedStruct = new Struct(expectedSchema)
                .put("b", List.of(expectednestedStruct));

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void renameNestedArrayField() {
        var transform = new ReplaceFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "replace", "b.inner_a:inner_a_renamed"
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

        var nestedStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", List.of(nestedStruct));

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedNestedSchema = SchemaBuilder
                .struct()
                .field("inner_a_renamed", Schema.STRING_SCHEMA)
                .field("inner_b", Schema.STRING_SCHEMA)
                .field("inner_c", Schema.STRING_SCHEMA)
                .build();
        var expectedSchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", SchemaBuilder.array(expectedNestedSchema).build())
                .build();

        var expectednestedStruct = new Struct(expectedNestedSchema)
                .put("inner_a_renamed", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "inner_c_value");
        var expectedStruct = new Struct(expectedSchema)
                .put("a", "a_field_value")
                .put("b", List.of(expectednestedStruct));

        assertEquals(expectedSchema, resultStruct.schema());
        assertEquals(expectedStruct, resultStruct);
    }
}
