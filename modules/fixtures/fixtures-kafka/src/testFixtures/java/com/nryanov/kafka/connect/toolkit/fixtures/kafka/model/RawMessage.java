package com.nryanov.kafka.connect.toolkit.fixtures.kafka.model;

import java.nio.charset.StandardCharsets;

public record RawMessage(byte[] key, byte[] value) {

    public String key2String() {
        return new String(key, StandardCharsets.UTF_8);
    }

    public String value2String() {
        return new String(value, StandardCharsets.UTF_8);
    }

    public String pretty() {
        return String.format("raw: [key]: %s; [value]: %s", key2String(), value2String());
    }
}
