package com.nryanov.kafka.connect.toolkit.fixtures.kafka.connect;

import com.nryanov.kafka.connect.toolkit.fixtures.debezium.DebeziumFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.KafkaFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.postgres.PostgresFixtureContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;

public class KafkaConnectFixtureContainer implements Startable {
    private final Network network;
    private final KafkaFixtureContainer kafka;
    private final PostgresFixtureContainer postgres;
    private final DebeziumFixtureContainer debezium;

    public KafkaConnectFixtureContainer() {
        this.network = Network.newNetwork();
        this.kafka = new KafkaFixtureContainer(network);
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

    @Override
    public void start() {
        kafka.start();
        postgres.start();

        debezium.withKafka(network, kafka.getAliasedBoostrapServers());
        debezium.dependsOn(kafka);

        debezium.start();
    }

    @Override
    public void stop() {
        debezium.stop();
        postgres.stop();
        kafka.stop();
    }
}
