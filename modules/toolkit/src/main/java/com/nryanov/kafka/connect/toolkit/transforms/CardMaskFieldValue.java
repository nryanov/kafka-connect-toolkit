package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class CardMaskFieldValue<R extends ConnectRecord<R>> implements Transformation<R> {
    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public R apply(R record) {
        return null;
    }
}
