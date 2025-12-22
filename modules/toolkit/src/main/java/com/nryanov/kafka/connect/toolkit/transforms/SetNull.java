package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public abstract class SetNull<R extends ConnectRecord<R>> implements Transformation<R> {
    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    public static class Key<R extends ConnectRecord<R>> extends SetNull<R> {
        @Override
        public R apply(R record) {
            if (record == null) {
                return null;
            }

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    null,
                    null,
                    record.valueSchema(),
                    record.value(),
                    record.timestamp()
            );
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends SetNull<R> {
        @Override
        public R apply(R record) {
            if (record == null) {
                return null;
            }

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    null,
                    null,
                    record.timestamp()
            );
        }
    }
}
