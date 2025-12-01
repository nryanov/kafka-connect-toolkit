package com.nryanov.kafka.connect.toolkit.fixtures.kafka_connect;

import com.nryanov.kafka.connect.toolkit.fixtures.debezium.DebeziumFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.KafkaFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.postgres.PostgresFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.schema_registry.SchemaRegistryFixtureContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;

public class KafkaConnectFixtureContainer implements Startable {
    private final Network network;
    private final KafkaFixtureContainer kafka;
    private final PostgresFixtureContainer postgres;
    private final DebeziumFixtureContainer debezium;
    private final SchemaRegistryFixtureContainer schemaRegistry;

    public KafkaConnectFixtureContainer() {
        this.network = Network.newNetwork();
        this.kafka = new KafkaFixtureContainer(network);
        this.schemaRegistry = new SchemaRegistryFixtureContainer(network);
        this.postgres = new PostgresFixtureContainer(network);
        this.debezium = new DebeziumFixtureContainer(network);
    }

    public KafkaFixtureContainer getKafka() {
        return kafka;
    }

    public PostgresFixtureContainer getPostgres() {
        return postgres;
    }

    public DebeziumFixtureContainer getDebezium() {
        return debezium;
    }

    public SchemaRegistryFixtureContainer getSchemaRegistry() {
        return schemaRegistry;
    }

    @Override
    public void start() {
        schemaRegistry.start();

        kafka.dependsOn(schemaRegistry);
        kafka.start();

        postgres.start();

        debezium.withKafka(network, kafka.getNetworkBoostrapServers());
        debezium.dependsOn(kafka);

        debezium.start();
    }

    @Override
    public void stop() {
        debezium.stop();
        schemaRegistry.stop();
        postgres.stop();
        kafka.stop();
    }
}
