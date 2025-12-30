package com.nryanov.kafka.connect.toolkit.core;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public abstract class AbstractBaseTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    @Override
    public void close() {

    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        if (!shouldProcess(record)) {
            return record;
        }

        var updatedKeySchema = keySchema(record);
        var updatedKey = key(record, updatedKeySchema);

        var updatedValueSchema = valueSchema(record);
        var updatedValue = value(record, updatedValueSchema);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                updatedKeySchema,
                updatedKey,
                updatedValueSchema,
                updatedValue,
                record.timestamp()
        );
    }

    protected Object key(R record, Schema updatedSchema) {
        return record.key();
    }

    protected Object value(R record, Schema updatedSchema) {
        return record.value();
    }

    protected Schema keySchema(R record) {
        return record.keySchema();
    }

    protected Schema valueSchema(R record) {
        return record.valueSchema();
    }

    protected boolean shouldProcess(R record) {
        return true;
    }
}
