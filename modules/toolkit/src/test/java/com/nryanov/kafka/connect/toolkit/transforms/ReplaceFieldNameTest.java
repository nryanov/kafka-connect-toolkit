package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.DefaultRecords;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class ReplaceFieldNameTest {
    @Test
    public void excludeFieldFromKey() {
        var transform = new ReplaceFieldName<SinkRecord>();
        transform.configure(Map.of(
                "key.exclude", "b"
        ));

        var keyStruct = DefaultRecords.createNestedKeyStruct();
        var valueStruct = DefaultRecords.createNestedValueStruct();

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultKeyStruct = requireStruct(result.key(), "test");

        assertNotNull(result.keySchema().field("a"));
        assertNull(result.keySchema().field("b"));
        assertNotNull(result.keySchema().field("c"));

        var innerKeyArraySchema = keyStruct.schema().field("c").schema().field("inner_c").schema().valueSchema();
        var expectedKeyStruct = new Struct(keyStruct.schema());
        expectedKeyStruct.put("a", "a_field_value");
        expectedKeyStruct.put("c", new Struct(keyStruct.schema().field("c").schema())
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", List.of(
                        new Struct(innerKeyArraySchema)
                                .put("inner_inner_a", "inner_inner_a_array_value")
                                .put("inner_inner_b", 3)
                ))

        );

        assertEquals(expectedKeyStruct.get("a"), resultKeyStruct.get("a"));
        assertThrows(DataException.class, () -> resultKeyStruct.get("b"));
        assertEquals(expectedKeyStruct.getStruct("c"), resultKeyStruct.getStruct("c"));
    }

    @Test
    public void includeSubsetOfFieldsFromKey() {
        var transform = new ReplaceFieldName<SinkRecord>();
        transform.configure(Map.of(
                "key.include", "c"
        ));

        var keyStruct = DefaultRecords.createNestedKeyStruct();
        var valueStruct = DefaultRecords.createNestedValueStruct();

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var resultKeyStruct = requireStruct(result.key(), "test");

        assertNull(result.keySchema().field("a"));
        assertNull(result.keySchema().field("b"));
        assertNotNull(result.keySchema().field("c"));

        var innerKeyArraySchema = keyStruct.schema().field("c").schema().field("inner_c").schema().valueSchema();

        var expectedKeyStruct = new Struct(keyStruct.schema());
        expectedKeyStruct.put("c", new Struct(keyStruct.schema().field("c").schema())
                .put("inner_a", "inner_a_value")
                .put("inner_b", "inner_b_value")
                .put("inner_c", List.of(
                        new Struct(innerKeyArraySchema)
                                .put("inner_inner_a", "inner_inner_a_array_value")
                                .put("inner_inner_b", 3)
                ))

        );

        assertEquals(expectedKeyStruct.getStruct("c"), resultKeyStruct.getStruct("c"));
    }
}
