package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.*;

public class NormalizeFieldValueTest {
    @Test
    public void changeCaseOfNestedFields() {
        var transform = new NormalizeFieldValue<SinkRecord>();
        transform.configure(Map.of(
                "key.fields", "a:UPPER,c.inner_a:UPPER,c.inner_c.inner_inner_a:UPPER",
                "value.fields", "b.inner_b.inner_inner_b:UPPER"
        ));

        var keyStruct = createKeyStruct();
        var valueStruct = createValueStruct();

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var keyStructResult = requireStruct(result.key(), "test");
        var valueStructResult = requireStruct(result.value(), "test");

        assertEquals("A_FIELD_VALUE", keyStructResult.get("a"));
        assertEquals("INNER_A_VALUE", keyStructResult.getStruct("c").get("inner_a"));
        assertEquals("inner_b_value", keyStructResult.getStruct("c").get("inner_b"));
        assertEquals("INNER_INNER_A_ARRAY_VALUE", requireStruct(keyStructResult.getStruct("c").getArray("inner_c").getFirst(), "test").get("inner_inner_a"));

        assertEquals("a_value", valueStructResult.get("a"));
        assertEquals("inner_inner_a_value", valueStructResult.getStruct("b").getStruct("inner_b").get("inner_inner_a"));
        assertEquals(List.of("FIRST", "SECOND", "THIRD"), valueStructResult.getStruct("b").getStruct("inner_b").get("inner_inner_b"));
    }

    @Test
    public void changeCaseOfPlainField() {
        var transform = new NormalizeFieldValue<SinkRecord>();
        transform.configure(Map.of(
                "key.fields", ":UPPER"
        ));

        var keySchema = Schema.STRING_SCHEMA;
        var key = "key";
        var valueSchema = Schema.STRING_SCHEMA;
        var value = "value";

        var record = new SinkRecord("topic", 1, keySchema, key, valueSchema, value, 0L);

        var result = transform.apply(record);

        assertEquals("KEY", result.key());
        assertEquals("value", result.value());
    }

    private Schema createKeySchema() {
        var innerKeyArraySchema = SchemaBuilder.struct()
                .field("inner_inner_a", Schema.STRING_SCHEMA)
                .field("inner_inner_b", Schema.INT32_SCHEMA)
                .build();

        return SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", SchemaBuilder.struct()
                        .field("inner_a", Schema.STRING_SCHEMA)
                        .field("inner_b", Schema.STRING_SCHEMA)
                        .field("inner_c", SchemaBuilder.array(innerKeyArraySchema).build()
                        )
                        .build()
                )
                .build();
    }

    private Schema createValueSchema() {
        return SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", SchemaBuilder.struct()
                        .field("inner_a", Schema.INT32_SCHEMA)
                        .field("inner_b", SchemaBuilder.struct()
                                .field("inner_inner_a", Schema.STRING_SCHEMA)
                                .field("inner_inner_b", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                                .build()
                        )
                        .build()
                )
                .build();
    }

    private Struct createKeyStruct() {
        var keySchema = createKeySchema();
        var innerKeyArraySchema = keySchema.field("c").schema().field("inner_c").schema().valueSchema();

        var keyStruct = new Struct(keySchema);
        keyStruct.put("a", "a_field_value");
        keyStruct.put("b", 1);
        keyStruct.put("c", new Struct(keySchema.field("c").schema())
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", List.of(
                        new Struct(innerKeyArraySchema)
                                .put("inner_inner_a", "inner_inner_a_array_value")
                                .put("inner_inner_b", 3)
                ))

        );

        return keyStruct;
    }

    private Struct createValueStruct() {
        var valueSchema = createValueSchema();
        var valueStruct = new Struct(valueSchema);
        valueStruct.put("a", "a_value");
        valueStruct.put("b",
                new Struct(valueSchema.field("b").schema())
                        .put("inner_a", 2)
                        .put("inner_b",
                                new Struct(valueSchema.field("b").schema().field("inner_b").schema())
                                        .put("inner_inner_a", "inner_inner_a_value")
                                        .put("inner_inner_b", List.of("first", "second", "third"))
                        )
        );

        return valueStruct;
    }
}
