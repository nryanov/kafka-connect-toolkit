package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CardMaskTest {
    @Test
    public void correctlyHandleNullPayload() {
        var transform = new CardMask.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field_1"
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
                .build();

        assertEquals(expectedSchema, result.keySchema());
        assertNull(result.key());
    }

    @Test
    public void maskFieldInKey() {
        var transform = new CardMask.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field"
        ));

        var schema = SchemaBuilder.struct().field("field", Schema.STRING_SCHEMA).build();
        var keyStruct = new Struct(schema).put("field", "4111 1111 1111 1111");
        var valueStruct = new Struct(schema).put("field", "4111 1111 1111 1111");

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultKeyStruct = requireStruct(result.key(), "test");
        var resultValueStruct = requireStruct(result.value(), "test");

        var expectedKeyStruct = new Struct(schema).put("field", "4111********1111");
        var expectedValueStruct = new Struct(schema).put("field", "4111 1111 1111 1111");

        assertEquals(expectedKeyStruct, resultKeyStruct);
        assertEquals(expectedValueStruct, resultValueStruct);
    }

    @Test
    public void maskFieldInValue() {
        var transform = new CardMask.Value<SinkRecord>();
        transform.configure(Map.of(
                "fields", "field"
        ));

        var schema = SchemaBuilder.struct().field("field", Schema.STRING_SCHEMA).build();
        var keyStruct = new Struct(schema).put("field", "4111 1111 1111 1111");
        var valueStruct = new Struct(schema).put("field", "4111 1111 1111 1111");

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultKeyStruct = requireStruct(result.key(), "test");
        var resultValueStruct = requireStruct(result.value(), "test");

        var expectedKeyStruct = new Struct(schema).put("field", "4111 1111 1111 1111");
        var expectedValueStruct = new Struct(schema).put("field", "4111********1111");

        assertEquals(expectedKeyStruct, resultKeyStruct);
        assertEquals(expectedValueStruct, resultValueStruct);
    }

    @Test
    public void maskNestedField() {
        var transform = new CardMask.Key<SinkRecord>();
        transform.configure(Map.of(
                "fields", "b.inner_a"
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
                .put("inner_a", "Some text with card number 4111 1111 1111 1111 which should be masked")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "Not masked card number 4111 1111 1111 1111 because this filed is not selected");
        var struct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", nestedStruct);

        var record = new SinkRecord("topic", 1, struct.schema(), struct, null, null, 0L);

        var result = transform.apply(record);
        var resultStruct = requireStruct(result.key(), "test");

        var expectedNestedStruct = new Struct(nestedSchema)
                .put("inner_a", "Some text with card number 4111********1111 which should be masked")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "Not masked card number 4111 1111 1111 1111 because this filed is not selected");
        var expectedStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", expectedNestedStruct);

        assertEquals(expectedStruct, resultStruct);
    }

    @Test
    public void throwErrorIfIncorrectLowerAndUpperBoundWereUsed() {
        var incorrectConfig = Map.of(
                "fields", "field",
                "masking.card-number-lower-bound", 10,
                "masking.card-number-upper-bound", 9
        );
        var transform = new CardMask.Key<SinkRecord>();

        assertThrows(IllegalArgumentException.class, () -> transform.configure(incorrectConfig));
    }
}
