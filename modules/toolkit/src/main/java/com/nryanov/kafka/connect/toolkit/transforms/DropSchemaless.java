package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;

public abstract class DropSchemaless<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
    public static class Key<R extends ConnectRecord<R>> extends DropSchemaless<R> {
        @Override
        public R apply(R record) {
            if (record == null || record.keySchema() == null) {
                return null;
            }

            return record;
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends DropSchemaless<R> {
        @Override
        public R apply(R record) {
            if (record == null || record.valueSchema() == null) {
                return null;
            }

            return record;
        }
    }
}
