package com.nryanov.kafka.connect.toolkit.fixtures.kafka.model;

public class RawMessage {
    public final byte[] key;
    public final byte[] value;

    public RawMessage(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }
}
