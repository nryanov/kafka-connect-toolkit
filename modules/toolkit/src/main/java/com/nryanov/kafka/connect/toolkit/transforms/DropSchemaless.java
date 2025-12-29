package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;

public abstract class DropSchemaless<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
    public static class Key<R extends ConnectRecord<R>> extends DropSchemaless<R> {
        @Override
        public R apply(R record) {
            return super.apply(record);
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends DropSchemaless<R> {
        @Override
        public R apply(R record) {
            return super.apply(record);
        }
    }
}
