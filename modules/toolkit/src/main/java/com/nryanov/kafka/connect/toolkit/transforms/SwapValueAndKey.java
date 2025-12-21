package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.Target;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class SwapValueAndKey<R extends ConnectRecord<R>> implements Transformation<R> {
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

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.valueSchema(),
                record.value(),
                record.keySchema(),
                record.key(),
                record.timestamp()
        );
    }
}
