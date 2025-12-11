package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class NormalizeFieldValueTest {
    @Test
    public void changeCaseOfNestedFields() {
        var transform = new NormalizeFieldValue<SinkRecord>();
        transform.configure(Map.of(
                "key.fields", "",
                "value.fields", ""
        ));

        var keySchema = SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", SchemaBuilder.struct()
                        .field("inner_a", Schema.STRING_SCHEMA)
                        .field("inner_b", Schema.STRING_SCHEMA)
                        .build()
                )
                .build();

        var keyStruct = new Struct(keySchema);
        keyStruct.put("a", "a_field_value");
        keyStruct.put("b", 1);
        keyStruct.put("c", new Struct(keySchema.field("c").schema())
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
        );

        var valueSchema = SchemaBuilder
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

        var record = new SinkRecord("topic", 1, keySchema, keyStruct, valueSchema, valueStruct, 0L);
        var result = transform.apply(record);
        System.out.println(result);
    }
}
