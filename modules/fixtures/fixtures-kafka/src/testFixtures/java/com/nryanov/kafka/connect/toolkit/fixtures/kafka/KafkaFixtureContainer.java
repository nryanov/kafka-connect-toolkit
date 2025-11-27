package com.nryanov.kafka.connect.toolkit.fixtures.kafka;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaFixtureContainer {
    private final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse(""));

    public void start() {
        kafka.start();
    }

    public void stop() {
        kafka.stop();
    }

    public String bootstrapServers() {
        if (!kafka.isRunning()) {
            throw new IllegalStateException("Kafka container is not running yet");
        }

        return kafka.getBootstrapServers();
    }
}
