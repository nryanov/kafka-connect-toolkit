package com.nryanov.kafka.connect.toolkit.debezium.converters;

import com.nryanov.kafka.connect.toolkit.fixtures.kafka_connect.KafkaConnectFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka_connect.helper.KafkaConnectDebeziumHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimestampConverterTest {
    private final static KafkaConnectFixtureContainer kafkaConnect = new KafkaConnectFixtureContainer();
    private static KafkaConnectDebeziumHelper debeziumHelper;

    private final static String CONNECTOR_NAME = "timestamp-converter-connector";

    @BeforeAll
    public static void setup() {
        kafkaConnect.bindConverterJarToPostgresConnector("build/libs", "debezium.jar");
        kafkaConnect.start();
        debeziumHelper = new KafkaConnectDebeziumHelper(kafkaConnect);

        debeziumHelper.setupSystemTables();
    }

    @AfterAll
    public static void close() {
        kafkaConnect.stop();
    }

    @BeforeEach
    public void before() {
        debeziumHelper.setupLogicalReplicationSlot();
    }

    @AfterEach
    public void after() {
        debeziumHelper.deleteConnector(CONNECTOR_NAME);
        debeziumHelper.dropLogicalReplicationSlot();
    }

    @Test
    public void debeziumPostgresConnectorWithoutTimestampConverterShouldNotChangeSchema() {
        var testName = "without_converter";

        debeziumHelper.setupPublication(testName);
        debeziumHelper.executeSql("""
                CREATE TABLE IF NOT EXISTS public.temporal
                (
                    id                                 BIGSERIAL PRIMARY KEY,

                    timestamp_no_tz                    TIMESTAMP WITHOUT TIME ZONE,
                    timestamp_no_tz_not_null           TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    timestamp_no_tz_not_null_default   TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    timestamp_no_tz_default            TIMESTAMP WITHOUT TIME ZONE          DEFAULT CURRENT_TIMESTAMP,

                    timestamp_with_tz                  TIMESTAMP WITH TIME ZONE,
                    timestamp_with_tz_not_null         TIMESTAMP WITH TIME ZONE    NOT NULL,
                    timestamp_with_tz_not_null_default TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    timestamp_with_tz_default          TIMESTAMP WITH TIME ZONE             DEFAULT CURRENT_TIMESTAMP,

                    date                               DATE,
                    date_not_null                      DATE                        NOT NULL,
                    date_not_null_default              DATE                        NOT NULL DEFAULT NOW(),
                    date_default                       DATE                                 DEFAULT NOW(),

                    time_no_tz                         TIME WITHOUT TIME ZONE,
                    time_no_tz_not_null                TIME WITHOUT TIME ZONE      NOT NULL,
                    time_no_tz_not_null_default        TIME WITHOUT TIME ZONE      NOT NULL DEFAULT CURRENT_TIME,
                    time_no_tz_default                 TIME WITHOUT TIME ZONE               DEFAULT CURRENT_TIME,

                    time_with_tz                       TIME WITH TIME ZONE,
                    time_with_tz_not_null              TIME WITH TIME ZONE         NOT NULL,
                    time_with_tz_not_null_default      TIME WITH TIME ZONE         NOT NULL DEFAULT CURRENT_TIME,
                    time_with_tz_default               TIME WITH TIME ZONE                  DEFAULT CURRENT_TIME
                );
                """);
        debeziumHelper.addTableToPublication(testName, "public.temporal");
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, testName, testName, ignored -> {});
        debeziumHelper.executeSql("""
                INSERT INTO public.temporal(timestamp_no_tz,
                                      timestamp_no_tz_not_null,
                                      timestamp_no_tz_not_null_default,
                                      timestamp_no_tz_default,
                                      timestamp_with_tz,
                                      timestamp_with_tz_not_null,
                                      timestamp_with_tz_not_null_default,
                                      timestamp_with_tz_default,
                                      date,
                                      date_not_null,
                                      date_not_null_default,
                                      date_default,
                                      time_no_tz,
                                      time_no_tz_not_null,
                                      time_no_tz_not_null_default,
                                      time_no_tz_default,
                                      time_with_tz,
                                      time_with_tz_not_null,
                                      time_with_tz_not_null_default,
                                      time_with_tz_default)
                VALUES (CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIME,
                        CURRENT_TIME,
                        CURRENT_TIME,
                        CURRENT_TIME,
                        CURRENT_TIME,
                        CURRENT_TIME,
                        CURRENT_TIME,
                        CURRENT_TIME);
                """);

        var msg = debeziumHelper.readAvroMessages(String.format("%s.public.temporal", testName), 1);
        assertEquals(1, msg.size());
        var schema = debeziumHelper.getLatestSchema(String.format("%s.public.temporal-value", testName));
        //TODO: validate schema
    }

    @Test
    public void debeziumPostgresConnectorWithTimestampConverterShouldChangeSchemaForTemporalTypes() {
        var testName = "with_converter";

        debeziumHelper.setupPublication(testName);
        debeziumHelper.executeSql("""
                CREATE TABLE IF NOT EXISTS public.temporal
                (
                    id                                 BIGSERIAL PRIMARY KEY,

                    timestamp_no_tz                    TIMESTAMP WITHOUT TIME ZONE,
                    timestamp_no_tz_not_null           TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    timestamp_no_tz_not_null_default   TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    timestamp_no_tz_default            TIMESTAMP WITHOUT TIME ZONE          DEFAULT CURRENT_TIMESTAMP,

                    timestamp_with_tz                  TIMESTAMP WITH TIME ZONE,
                    timestamp_with_tz_not_null         TIMESTAMP WITH TIME ZONE    NOT NULL,
                    timestamp_with_tz_not_null_default TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    timestamp_with_tz_default          TIMESTAMP WITH TIME ZONE             DEFAULT CURRENT_TIMESTAMP,

                    date                               DATE,
                    date_not_null                      DATE                        NOT NULL,
                    date_not_null_default              DATE                        NOT NULL DEFAULT NOW(),
                    date_default                       DATE                                 DEFAULT NOW(),

                    time_no_tz                         TIME WITHOUT TIME ZONE,
                    time_no_tz_not_null                TIME WITHOUT TIME ZONE      NOT NULL,
                    time_no_tz_not_null_default        TIME WITHOUT TIME ZONE      NOT NULL DEFAULT CURRENT_TIME,
                    time_no_tz_default                 TIME WITHOUT TIME ZONE               DEFAULT CURRENT_TIME,

                    time_with_tz                       TIME WITH TIME ZONE,
                    time_with_tz_not_null              TIME WITH TIME ZONE         NOT NULL,
                    time_with_tz_not_null_default      TIME WITH TIME ZONE         NOT NULL DEFAULT CURRENT_TIME,
                    time_with_tz_default               TIME WITH TIME ZONE                  DEFAULT CURRENT_TIME
                );
                """);
        debeziumHelper.addTableToPublication(testName, "public.temporal");
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, testName, testName, connector -> {
            connector.with("converters", "timestampConverter");
            connector.with("timestampConverter.type", "com.nryanov.kafka.connect.toolkit.debezium.converters.TimestampConverter");
        });
        debeziumHelper.executeSql("""
                INSERT INTO public.temporal(timestamp_no_tz,
                                      timestamp_no_tz_not_null,
                                      timestamp_no_tz_not_null_default,
                                      timestamp_no_tz_default,
                                      timestamp_with_tz,
                                      timestamp_with_tz_not_null,
                                      timestamp_with_tz_not_null_default,
                                      timestamp_with_tz_default,
                                      date,
                                      date_not_null,
                                      date_not_null_default,
                                      date_default,
                                      time_no_tz,
                                      time_no_tz_not_null,
                                      time_no_tz_not_null_default,
                                      time_no_tz_default,
                                      time_with_tz,
                                      time_with_tz_not_null,
                                      time_with_tz_not_null_default,
                                      time_with_tz_default)
                VALUES (CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIME,
                        CURRENT_TIME,
                        CURRENT_TIME,
                        CURRENT_TIME,
                        CURRENT_TIME,
                        CURRENT_TIME,
                        CURRENT_TIME,
                        CURRENT_TIME);
                """);

        var msg = debeziumHelper.readAvroMessages(String.format("%s.public.temporal", testName), 1);
        assertEquals(1, msg.size());
        var schema = debeziumHelper.getLatestSchema(String.format("%s.public.temporal-value", testName));
        //TODO: validate schema
    }
}
