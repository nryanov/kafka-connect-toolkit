package com.nryanov.kafka.connect.toolkit.fixtures.debezium;


import io.debezium.testing.testcontainers.DebeziumContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;

import java.util.List;
import java.util.Set;

public class DebeziumFixtureContainer implements Startable {
    private final DebeziumContainer debezium;

    public DebeziumFixtureContainer(Network network) {
        this();
        debezium.withNetwork(network);
    }

    public DebeziumFixtureContainer() {
        debezium = new DebeziumContainer("quay.io/debezium/connect:3.3.1.Final")
                .enableApicurioConverters();
    }

    @Override
    public void start() {
        debezium.start();
    }

    @Override
    public void stop() {
        debezium.stop();
    }

    @Override
    public Set<Startable> getDependencies() {
        return debezium.getDependencies();
    }

    public DebeziumContainer getContainer() {
        return debezium;
    }

    public void withKafka(Network network, String bootstrapServers) {
        debezium.withKafka(network, bootstrapServers);
    }

    public void dependsOn(Startable... startables) {
        debezium.dependsOn(startables);
    }

    public List<String> getRegisteredConnectors() {
        return debezium.getRegisteredConnectors();
    }
}
