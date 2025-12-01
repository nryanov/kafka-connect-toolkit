package com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper;

import com.nryanov.kafka.connect.toolkit.fixtures.kafka.KafkaFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.model.AvroMessage;
import com.nryanov.kafka.connect.toolkit.fixtures.schema_registry.SchemaRegistryFixtureContainer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaAvroConsumerHelper {
    private final static Duration DEFAULT_POLL_DURATION = Duration.ofSeconds(1);
    private final Consumer<GenericRecord, GenericRecord> avroConsumer;

    public KafkaAvroConsumerHelper(KafkaFixtureContainer kafka, SchemaRegistryFixtureContainer schemaRegistry) {
        var avroConsumerProperties = new Properties();
        avroConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        avroConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        avroConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        avroConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        avroConsumerProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.confluentUrl());

        this.avroConsumer = new KafkaConsumer<>(avroConsumerProperties);
    }

    public List<AvroMessage> read(String topic, int limit, Duration timeout) {
        var partitions = avroConsumer.partitionsFor(topic);
        var metadata = partitions.stream().map(it -> new TopicPartition(it.topic(), it.partition())).toList();
        avroConsumer.assign(metadata);

        var result = new ArrayList<AvroMessage>();

        var start = System.currentTimeMillis();
        while (result.size() < limit && timeout.compareTo(Duration.ofMillis(System.currentTimeMillis() - start)) > 0) {
            var messages = avroConsumer.poll(DEFAULT_POLL_DURATION);
            messages.forEach(it -> result.add(new AvroMessage(it.key(), it.value())));
        }

        return result;
    }
}
