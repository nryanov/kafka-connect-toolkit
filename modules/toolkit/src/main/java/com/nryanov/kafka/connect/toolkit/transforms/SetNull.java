package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.core.AbstractBaseTransform;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;

public abstract class SetNull<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
    public static class Key<R extends ConnectRecord<R>> extends SetNull<R> {
        @Override
        protected Schema keySchema(R record) {
            return null;
        }

        @Override
        protected Object key(R record, Schema updatedSchema) {
            return null;
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends SetNull<R> {
        @Override
        protected Object value(R record, Schema updatedSchema) {
            return null;
        }

        @Override
        protected Schema valueSchema(R record) {
            return null;
        }
    }
}
