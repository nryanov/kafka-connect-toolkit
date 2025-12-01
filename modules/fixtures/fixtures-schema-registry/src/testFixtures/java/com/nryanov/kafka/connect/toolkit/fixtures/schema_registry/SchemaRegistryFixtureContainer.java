package com.nryanov.kafka.connect.toolkit.fixtures.schema_registry;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.util.Set;

public class SchemaRegistryFixtureContainer implements Startable {
    public final static String NETWORK_ALIAS = "schema-registry";

    private final GenericContainer<?> schemaRegistry;

    public SchemaRegistryFixtureContainer(Network network) {
        this();
        schemaRegistry.withNetwork(network);
    }

    public SchemaRegistryFixtureContainer() {
        this.schemaRegistry = new GenericContainer<>(DockerImageName.parse("apicurio/apicurio-registry:3.0.9"))
                .withExposedPorts(8080)
                .withNetworkAliases(NETWORK_ALIAS);
    }

    @Override
    public void start() {
        schemaRegistry.start();
    }

    @Override
    public void stop() {
        schemaRegistry.stop();
    }

    @Override
    public Set<Startable> getDependencies() {
        return schemaRegistry.getDependencies();
    }

    public String url() {
        return String.format("http://%s:%s", schemaRegistry.getHost(), schemaRegistry.getMappedPort(8080));
    }

    public String confluentUrl() {
        return String.format("http://%s:%s/apis/ccompat/v7", schemaRegistry.getHost(), schemaRegistry.getMappedPort(8080));
    }

    public String networkUrl() {
        return String.format("http://%s:%s/apis/registry/v2", NETWORK_ALIAS, "8080");
    }
}
