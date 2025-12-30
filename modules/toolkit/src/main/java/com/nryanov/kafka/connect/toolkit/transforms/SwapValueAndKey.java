package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.core.AbstractBaseTransform;
import org.apache.kafka.connect.connector.ConnectRecord;

public class SwapValueAndKey<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
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
