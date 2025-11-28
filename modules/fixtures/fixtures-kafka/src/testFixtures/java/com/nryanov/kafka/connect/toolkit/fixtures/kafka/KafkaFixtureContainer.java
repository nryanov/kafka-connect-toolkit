package com.nryanov.kafka.connect.toolkit.fixtures.kafka;

import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Set;

public class KafkaFixtureContainer implements Startable {
    private final RedpandaContainer redpanda;

    public KafkaFixtureContainer(Network network) {
        this();
        redpanda.withNetwork(network);
    }

    public KafkaFixtureContainer() {
        redpanda = new RedpandaContainer(DockerImageName.parse("redpandadata/redpanda:v24.2.25"));
    }

    @Override
    public void start() {
        redpanda.start();
    }

    @Override
    public void stop() {
        redpanda.stop();
    }

    @Override
    public Set<Startable> getDependencies() {
        return redpanda.getDependencies();
    }

    public String bootstrapServers() {
        if (!redpanda.isRunning()) {
            throw new IllegalStateException("Redpanda container is not running yet");
        }

        return redpanda.getBootstrapServers();
    }

    public String getAliasedBoostrapServers() {
        if (!redpanda.isRunning()) {
            throw new IllegalStateException("Redpanda container is not running yet");
        }

        return redpanda.getNetworkAliases().getFirst() + ":29092";
    }
}
