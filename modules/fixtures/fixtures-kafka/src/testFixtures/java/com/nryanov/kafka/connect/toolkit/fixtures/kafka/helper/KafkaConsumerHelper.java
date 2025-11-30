package com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper;

import com.nryanov.kafka.connect.toolkit.fixtures.kafka.KafkaFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.model.RawMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerHelper {
    private final static Duration DEFAULT_POLL_DURATION = Duration.ofSeconds(1);
    private final Consumer<byte[], byte[]> consumer;

    public KafkaConsumerHelper(KafkaFixtureContainer container) {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, container.bootstrapServers());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(properties);
    }

    public List<RawMessage> read(String topic, int limit, Duration timeout) {
        var partitions = consumer.partitionsFor(topic);
        var metadata = partitions.stream().map(it -> new TopicPartition(it.topic(), it.partition())).toList();
        consumer.assign(metadata);

        var result = new ArrayList<RawMessage>();

        var start = System.currentTimeMillis();
        while (result.size() < limit && timeout.compareTo(Duration.ofMillis(System.currentTimeMillis() - start)) > 0) {
            var messages = consumer.poll(DEFAULT_POLL_DURATION);
            messages.forEach(it -> result.add(new RawMessage(it.key(), it.value())));
        }

        return result;
    }
}
