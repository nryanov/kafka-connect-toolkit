package com.nryanov.kafka.connect.toolkit.fixtures.kafka.model;

import org.apache.avro.generic.GenericRecord;

public record AvroMessage(GenericRecord key, GenericRecord value) {
    public String pretty() {
        return String.format("avro: [key]: %s, [value]: %s", key.toString(), value.toString());
    }

    public GenericRecord getNestedRecord(String name) {
        return (GenericRecord) value.get(name);
    }
}
