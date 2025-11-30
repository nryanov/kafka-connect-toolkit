package com.nryanov.kafka.connect.toolkit.debezium.converters;

import com.nryanov.kafka.connect.toolkit.fixtures.debezium.MockConverterRegistry;
import com.nryanov.kafka.connect.toolkit.fixtures.debezium.MockRelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimestampConverterTest {
    @Test
    public void correctlyConvertTimestampFromLong() {
        var converter = new TimestampConverter();

        var column = new MockRelationalColumn("timestamp", "timestamp");
        var converterRegistry = new MockConverterRegistry<SchemaBuilder>();

        converter.converterFor(column, converterRegistry);

        var instant = LocalDateTime.of(2025, 11, 27, 12, 10, 20);
        var result = converterRegistry.converter.convert(instant.toEpochSecond(ZoneOffset.UTC));

        assertEquals("2025-11-27T12:10:20.000", result);
    }
}
