package com.nryanov.kafka.connect.toolkit.debezium.transforms;

import com.nryanov.kafka.connect.toolkit.fixtures.kafka_connect.KafkaConnectFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka_connect.helper.KafkaConnectDebeziumHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaRenameTest {
    private final static KafkaConnectFixtureContainer kafkaConnect = new KafkaConnectFixtureContainer();
    private static KafkaConnectDebeziumHelper debeziumHelper;

    private final static String CONNECTOR_NAME = "schema-rename-connector";

    @BeforeAll
    public static void setup() {
        kafkaConnect.bindTransformJar("build/libs", "debezium.jar");
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
    public void connectorWithoutTransformation() {
        var publication = "without_transformation";
        var topicPrefix = "without_transformation";

        var topic = topicPrefix + ".public.data";
        var valueSubject = topic + "-value";
        var keySubject = topic + "-key";

        debeziumHelper.setupPublication(publication);
        debeziumHelper.executeSql("CREATE TABLE IF NOT EXISTS public.data(id BIGSERIAL PRIMARY KEY, value TEXT)");
        debeziumHelper.addTableToPublication(publication, "public.data");
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, topicPrefix, publication, ignored -> {});
        debeziumHelper.executeSql("INSERT INTO public.data(value) VALUES('some-data')");

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        assertEquals(1, msg.size());

        var valueSchema = debeziumHelper.getLatestSchema(valueSubject);
        var keySchema = debeziumHelper.getLatestSchema(keySubject);

        assertEquals("without_transformation.public.data.Key", keySchema.schema().getFullName());
        assertEquals("without_transformation.public.data.Envelope", valueSchema.schema().getFullName());
        assertEquals("without_transformation.public.data.Value", valueSchema.before().getFullName());
        assertEquals("without_transformation.public.data.Value", valueSchema.after().getFullName());
    }

    @Test
    public void connectorWithTransformationShouldRenameInternalSchemas() {
        var publication = "with_internal_transformation";
        var topicPrefix = "with_internal_transformation";

        var topic = topicPrefix + ".public.data";
        var valueSubject = topic + "-value";
        var keySubject = topic + "-key";

        debeziumHelper.setupPublication(publication);
        debeziumHelper.executeSql("CREATE TABLE If NOT EXISTS public.data(id BIGSERIAL PRIMARY KEY, value TEXT)");
        debeziumHelper.addTableToPublication(publication, "public.data");

        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(
                CONNECTOR_NAME,
                topicPrefix,
                publication,
                connector -> {
                    connector.with("transforms", "schemaRename");
                    connector.with("transforms.schemaRename.type", "com.nryanov.kafka.connect.toolkit.debezium.transforms.SchemaRename");
                    connector.with("transforms.schemaRename.internal.name", "new_internal_schema.internal_payload");
                    connector.with("transforms.schemaRename.cache.size", 32);
                }
        );

        debeziumHelper.executeSql("INSERT INTO public.data(value) VALUES('some-data')");

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        assertEquals(1, msg.size());

        var valueSchema = debeziumHelper.getLatestSchema(valueSubject);
        var keySchema = debeziumHelper.getLatestSchema(keySubject);

        assertEquals("with_internal_transformation.public.data.Key", keySchema.schema().getFullName());
        assertEquals("with_internal_transformation.public.data.Envelope", valueSchema.schema().getFullName());
        assertEquals("new_internal_schema.internal_payload", valueSchema.before().getFullName());
        assertEquals("new_internal_schema.internal_payload", valueSchema.after().getFullName());
    }

    @Test
    public void connectorWithTransformationShouldNotRenameAnySchemas() {
        var publication = "with_disabled_transformation";
        var topicPrefix = "with_disabled_transformation";

        var topic = topicPrefix + ".public.data";
        var valueSubject = topic + "-value";
        var keySubject = topic + "-key";

        debeziumHelper.setupPublication(publication);
        debeziumHelper.executeSql("CREATE TABLE If NOT EXISTS public.data(id BIGSERIAL PRIMARY KEY, value TEXT)");
        debeziumHelper.addTableToPublication(publication, "public.data");

        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(
                CONNECTOR_NAME,
                topicPrefix,
                publication,
                connector -> {
                    connector.with("transforms", "schemaRename");
                    connector.with("transforms.schemaRename.type", "com.nryanov.kafka.connect.toolkit.debezium.transforms.SchemaRename");
                    connector.with("transforms.schemaRename.cache.size", 32);
                }
        );

        debeziumHelper.executeSql("INSERT INTO public.data(value) VALUES('some-data')");

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        assertEquals(1, msg.size());

        var valueSchema = debeziumHelper.getLatestSchema(valueSubject);
        var keySchema = debeziumHelper.getLatestSchema(keySubject);

        assertEquals("with_disabled_transformation.public.data.Key", keySchema.schema().getFullName());
        assertEquals("with_disabled_transformation.public.data.Envelope", valueSchema.schema().getFullName());
        assertEquals("with_disabled_transformation.public.data.Value", valueSchema.before().getFullName());
        assertEquals("with_disabled_transformation.public.data.Value", valueSchema.after().getFullName());
    }
}
