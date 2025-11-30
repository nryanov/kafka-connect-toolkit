package com.nryanov.kafka.connect.toolkit.debezium.transforms;

import com.nryanov.kafka.connect.toolkit.fixtures.debezium.DebeziumFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.jars.JarBuilder;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.KafkaFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaAdminHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaConsumerHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaProducerHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.postgres.PostgresFixtureContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.time.Duration;

public class SchemaRenameTest {
    private final static Network network = Network.newNetwork();

    private final static PostgresFixtureContainer postgres = new PostgresFixtureContainer(network);
    private final static KafkaFixtureContainer kafka = new KafkaFixtureContainer(network);
    private final static DebeziumFixtureContainer debezium = new DebeziumFixtureContainer(network);

    @BeforeAll
    public static void setup() {
        postgres.start();
        kafka.start();

        debezium.withKafka(network, kafka.getAliasedBoostrapServers());
        debezium.dependsOn(kafka);
        debezium.start();
    }

    @AfterAll
    public static void close() {
        debezium.stop();
        postgres.stop();
        kafka.stop();
    }
}
