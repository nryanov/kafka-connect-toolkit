package com.nryanov.kafka.connect.toolkit.core;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;
import java.util.function.Supplier;

public class CacheableTransform<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
    private final static int DEFAULT_MAX_CACHE_SIZE = 32;
    private Cache<Schema, Schema> schemaCache;

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        schemaCache = new SynchronizedCache<>(new LRUCache<>(DEFAULT_MAX_CACHE_SIZE));
    }

    protected Schema getOrCompute(Schema source, Supplier<Schema> computed) {
        var cached = schemaCache.get(source);

        if (cached == null) {
            cached = computed.get();
            schemaCache.put(source, cached);
        }

        return cached;
    }
}
