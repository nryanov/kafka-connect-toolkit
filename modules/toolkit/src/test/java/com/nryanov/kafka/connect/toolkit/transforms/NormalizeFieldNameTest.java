package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.DefaultRecords;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class NormalizeFieldNameTest {
    @Test
    public void transformNamesToUpperCase() {
        var transform = new NormalizeFieldName<SinkRecord>();
        transform.configure(Map.of(
                "case.initial", "LOWER_UNDERSCORE",
                "case.target", "LOWER_CAMEL"
        ));

        var keyStruct = DefaultRecords.createNestedKeyStruct();
        var valueStruct = DefaultRecords.createNestedValueStruct();

        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);

        var expectedKeySchema = DefaultRecords.createNestedKeySchemaLowerCamelFieldNames();
        var expectedValueSchema = DefaultRecords.createNestedValueSchemaLowerCamelFieldNames();

        assertEquals(expectedKeySchema, result.keySchema());
        assertEquals(expectedValueSchema, result.valueSchema());
    }
}
