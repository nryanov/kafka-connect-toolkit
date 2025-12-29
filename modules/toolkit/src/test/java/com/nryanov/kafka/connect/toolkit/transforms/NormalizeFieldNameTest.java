package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.DefaultRecords;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class NormalizeFieldNameTest {
    @Test
    public void correctlyHandleNullPayload() {
        var transform = new NormalizeFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "case.initial", "LOWER_UNDERSCORE",
                "case.target", "LOWER_CAMEL"
        ));

        var schema = Schema.STRING_SCHEMA;

        var record = new SinkRecord("topic", 1, schema, null, null, null, 0L);
        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void correctlyHandleNullSchema() {
        var transform = new NormalizeFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "case.initial", "LOWER_UNDERSCORE",
                "case.target", "LOWER_CAMEL"
        ));

        var record = new SinkRecord("topic", 1, null, "value", null, null, 0L);
        assertDoesNotThrow(() -> transform.apply(record));
    }

    @Test
    public void transformNamesToUpperCaseInKey() {
        var transform = new NormalizeFieldName.Key<SinkRecord>();
        transform.configure(Map.of(
                "case.initial", "LOWER_UNDERSCORE",
                "case.target", "LOWER_CAMEL"
        ));

        var keyStruct = DefaultRecords.createNestedKeyStruct();
        var record = new SinkRecord("topic", 1, keyStruct.schema(), keyStruct, null, null, 0L);

        var result = transform.apply(record);
        var expectedKeySchema = DefaultRecords.createNestedKeySchemaLowerCamelFieldNames();

        assertEquals(expectedKeySchema, result.keySchema());
    }

    @Test
    public void transformNamesToUpperCaseInValue() {
        var transform = new NormalizeFieldName.Value<SinkRecord>();
        transform.configure(Map.of(
                "case.initial", "LOWER_UNDERSCORE",
                "case.target", "LOWER_CAMEL"
        ));

        var valueStruct = DefaultRecords.createNestedValueStruct();
        var record = new SinkRecord("topic", 1, null, null, valueStruct.schema(), valueStruct, 0L);

        var result = transform.apply(record);
        var expectedValueSchema = DefaultRecords.createNestedValueSchemaLowerCamelFieldNames();

        assertEquals(expectedValueSchema, result.valueSchema());
    }
}
