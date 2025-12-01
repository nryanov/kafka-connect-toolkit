package com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper;

import com.nryanov.kafka.connect.toolkit.fixtures.kafka.KafkaFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.schema_registry.SchemaRegistryFixtureContainer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaAvroProducerHelper {
    private final static long DEFAULT_WAIT_TIMEOUT = 5;

    private final Producer<GenericRecord, GenericRecord> producer;

    public KafkaAvroProducerHelper(KafkaFixtureContainer kafka, SchemaRegistryFixtureContainer schemaRegistry) {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "50");
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.confluentUrl());

        this.producer = new KafkaProducer<>(properties);
    }

    public RecordMetadata send(String topic, GenericRecord value) {
        return send(topic, null, value);
    }

    public RecordMetadata send(String topic, GenericRecord key, GenericRecord value) {
        try {
            var record = new ProducerRecord<>(topic, key, value);
            return producer.send(record).get(DEFAULT_WAIT_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
