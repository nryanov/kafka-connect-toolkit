package com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper;

import com.nryanov.kafka.connect.toolkit.fixtures.kafka.KafkaFixtureContainer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaRawProducerHelper {
    private final static long DEFAULT_WAIT_TIMEOUT = 5;

    private final Producer<byte[], byte[]> producer;

    public KafkaRawProducerHelper(KafkaFixtureContainer container) {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, container.bootstrapServers());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "50");

        this.producer = new KafkaProducer<>(properties);
    }

    public RecordMetadata send(String topic, byte[] value) {
        return send(topic, null, value);
    }

    public RecordMetadata send(String topic, byte[] key, byte[] value) {
        try {
            var record = new ProducerRecord<>(topic, key, value);
            return producer.send(record).get(DEFAULT_WAIT_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
