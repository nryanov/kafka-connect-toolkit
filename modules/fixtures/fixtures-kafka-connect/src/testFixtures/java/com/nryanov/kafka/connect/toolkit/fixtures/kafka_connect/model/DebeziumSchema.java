package com.nryanov.kafka.connect.toolkit.fixtures.kafka_connect.model;

import org.apache.avro.Schema;

public record DebeziumSchema(Schema schema) {
    public Schema before() {
        return schema.getField("before").schema().getTypes().getLast();
    }

    public Schema after() {
        return schema.getField("after").schema().getTypes().getLast();
    }
}
