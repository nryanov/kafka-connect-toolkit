package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class NormalizeFieldNameTest {
    @Test
    public void transformNamesToUpperCase() {
        var transform = new NormalizeFieldName<SinkRecord>();
        transform.configure(Map.of(
                "case.initial", "LOWER",
                "case.target", "UPPER"
        ));

        var keyStruct = createKeyStruct();
        var valueStruct = createValueStruct();

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        System.out.println(result.key());
        System.out.println(result.keySchema());

        System.out.println(result.value());
        System.out.println(result.valueSchema());

    }

    private Schema createKeySchema() {
        var innerKeyArraySchema = SchemaBuilder.struct()
                .field("inner_INNER_a", Schema.STRING_SCHEMA)
                .field("INNER_inner_B", Schema.INT32_SCHEMA)
                .build();

        return SchemaBuilder
                .struct()
                .field("A", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("C", SchemaBuilder.struct()
                        .field("inner_A", Schema.STRING_SCHEMA)
                        .field("INNER_b", Schema.STRING_SCHEMA)
                        .field("InNeR_c", SchemaBuilder.array(innerKeyArraySchema).build()
                        )
                        .build()
                )
                .build();
    }

    private Schema createValueSchema() {
        return SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("B", SchemaBuilder.struct()
                        .field("iNnEr_A", Schema.INT32_SCHEMA)
                        .field("INNER_b", SchemaBuilder.struct()
                                .field("INNER_inner_A", Schema.STRING_SCHEMA)
                                .field("inner_INNER_b", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                                .build()
                        )
                        .build()
                )
                .build();
    }

    private Struct createKeyStruct() {
        var keySchema = createKeySchema();
        var innerKeyArraySchema = keySchema.field("C").schema().field("InNeR_c").schema().valueSchema();

        var keyStruct = new Struct(keySchema);
        keyStruct.put("A", "a_field_value");
        keyStruct.put("b", 1);
        keyStruct.put("C", new Struct(keySchema.field("C").schema())
                .put("inner_A", "inner_a_value")
                .put("INNER_b", "inner_b_value")
                .put("InNeR_c", List.of(
                        new Struct(innerKeyArraySchema)
                                .put("inner_INNER_a", "inner_inner_a_array_value")
                                .put("INNER_inner_B", 3)
                ))

        );

        return keyStruct;
    }

    private Struct createValueStruct() {
        var valueSchema = createValueSchema();
        var valueStruct = new Struct(valueSchema);
        valueStruct.put("a", "a_value");
        valueStruct.put("B",
                new Struct(valueSchema.field("B").schema())
                        .put("iNnEr_A", 2)
                        .put("INNER_b",
                                new Struct(valueSchema.field("B").schema().field("INNER_b").schema())
                                        .put("INNER_inner_A", "inner_inner_a_value")
                                        .put("inner_INNER_b", List.of("first", "second", "third"))
                        )
        );

        return valueStruct;
    }
}
