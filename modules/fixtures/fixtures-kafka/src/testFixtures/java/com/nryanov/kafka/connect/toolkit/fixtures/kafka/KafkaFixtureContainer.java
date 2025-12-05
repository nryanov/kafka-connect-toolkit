package com.nryanov.kafka.connect.toolkit.fixtures.kafka;

import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Set;

public class KafkaFixtureContainer implements Startable {
    public final String NETWORK_ALIAS = "redpanda";
    private final RedpandaContainer redpanda;

    public KafkaFixtureContainer(Network network) {
        this();
        redpanda.withNetwork(network);
    }

    public KafkaFixtureContainer() {
        redpanda = new RedpandaContainer(DockerImageName.parse("redpandadata/redpanda:v25.3.1"))
                .withNetworkAliases(NETWORK_ALIAS)
                .withListener("redpanda:29092");
    }

    public void dependsOn(Startable... dependencies) {
        redpanda.dependsOn(dependencies);
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

    public String getNetworkBoostrapServers() {
        if (!redpanda.isRunning()) {
            throw new IllegalStateException("Redpanda container is not running yet");
        }

        return NETWORK_ALIAS + ":29092";
    }
}
