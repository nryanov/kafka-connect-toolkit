package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CardMaskFieldValueTest {
    @Test
    public void maskOnlyKeyField() {
        var transform = new CardMaskFieldValue<SinkRecord>();
        transform.configure(Map.of(
                "key.fields", "field"
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
    public void maskOnlyValueField() {
        var transform = new CardMaskFieldValue<SinkRecord>();
        transform.configure(Map.of(
                "value.fields", "field"
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
        var transform = new CardMaskFieldValue<SinkRecord>();
        transform.configure(Map.of(
                "key.fields", "b.inner_a",
                "value.fields", "b.inner_c"
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
                .put("inner_a", "Some text with card number 4111 1111 1111 1111 which should be masked")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "Not masked card number 4111 1111 1111 1111 because this filed is not selected");
        var keyStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", nestedKeyStruct);

        var nestedValueStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "4111 1111 1111 1111");
        var valueStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", nestedValueStruct);

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultValueStruct = requireStruct(result.value(), "test");
        var resultKeyStruct = requireStruct(result.key(), "test");

        var expectedNestedKeyStruct = new Struct(nestedSchema)
                .put("inner_a", "Some text with card number 4111********1111 which should be masked")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "Not masked card number 4111 1111 1111 1111 because this filed is not selected");
        var expectedKeyStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", expectedNestedKeyStruct);

        var expectedNestedValueStruct = new Struct(nestedSchema)
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", "4111********1111");
        var expectedValueStruct = new Struct(schema)
                .put("a", "a_field_value")
                .put("b", expectedNestedValueStruct);

        assertEquals(expectedKeyStruct, resultKeyStruct);
        assertEquals(expectedValueStruct, resultValueStruct);
    }

    @Test
    public void throwErrorIfIncorrectLowerAndUpperBoundWereUsed() {
        var incorrectConfig = Map.of(
                "value.fields", "field",
                "masking.card-number-lower-bound", 10,
                "masking.card-number-upper-bound", 9
        );
        var transform = new CardMaskFieldValue<SinkRecord>();

        assertThrows(IllegalArgumentException.class, () -> transform.configure(incorrectConfig));
    }
}
