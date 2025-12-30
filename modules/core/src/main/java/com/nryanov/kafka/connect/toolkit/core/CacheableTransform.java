package com.nryanov.kafka.connect.toolkit.core;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

public class CacheableTransform<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
    protected Cache<Schema, Schema> schemaCache;
}
