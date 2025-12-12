package com.nryanov.kafka.connect.toolkit.transforms.common;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.List;

public class DefaultRecords {
    public static Schema createNestedKeySchemaLowerCamelFieldNames() {
        var innerKeyArraySchema = SchemaBuilder.struct()
                .field("innerInnerA", Schema.STRING_SCHEMA)
                .field("innerInnerB", Schema.INT32_SCHEMA)
                .build();

        return SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", SchemaBuilder.struct()
                        .field("innerA", Schema.STRING_SCHEMA)
                        .field("innerB", Schema.STRING_SCHEMA)
                        .field("innerC", SchemaBuilder.array(innerKeyArraySchema).build()
                        )
                        .build()
                )
                .build();
    }

    public static Schema createNestedValueSchemaLowerCamelFieldNames() {
        return SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", SchemaBuilder.struct()
                        .field("innerA", Schema.INT32_SCHEMA)
                        .field("innerB", SchemaBuilder.struct()
                                .field("innerInnerA", Schema.STRING_SCHEMA)
                                .field("innerInnerB", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                                .build()
                        )
                        .build()
                )
                .build();
    }

    public static Schema createNestedKeySchemaLowerUnderscoreFieldNames() {
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

    public static Schema createNestedValueSchemaLowerUnderscoreFieldNames() {
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

    public static Struct createNestedKeyStruct() {
        var keySchema = createNestedKeySchemaLowerUnderscoreFieldNames();
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

    public static Struct createNestedValueStruct() {
        var valueSchema = createNestedValueSchemaLowerUnderscoreFieldNames();
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
