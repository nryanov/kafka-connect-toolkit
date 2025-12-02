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
        kafkaConnect.bindJarsToPostgresDebezium("build/libs", "debezium.jar");
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
        debeziumHelper.setupPublication(publication);
        debeziumHelper.executeSql("CREATE TABLE IF NOT EXISTS public.data(id BIGSERIAL PRIMARY KEY, value TEXT)");
        debeziumHelper.addTableToPublication(publication, "public.data");
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, "without-transformation", publication, ignored -> {});
        debeziumHelper.executeSql("INSERT INTO public.data(value) VALUES('some-data')");

        var msg = debeziumHelper.readAvroMessages("without-transformation.public.data", 1);
        assertEquals(1, msg.size());

        var schema = debeziumHelper.getLatestSchema("without-transformation.public.data-value");

        assertEquals("without_transformation.public.data.Envelope", schema.getFullName());
        assertEquals("without_transformation.public.data.Value", schema.getField("before").schema().getTypes().getLast().getFullName());
        assertEquals("without_transformation.public.data.Value", schema.getField("after").schema().getTypes().getLast().getFullName());
    }

    @Test
    public void connectorWithTransformationShouldRenameNestedSchemas() {
        var publication = "with_transformation";
        debeziumHelper.setupPublication(publication);
        debeziumHelper.executeSql("CREATE TABLE If NOT EXISTS public.data(id BIGSERIAL PRIMARY KEY, value TEXT)");
        debeziumHelper.addTableToPublication(publication, "public.data");

        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(
                CONNECTOR_NAME,
                "with-transformation",
                publication,
                connector -> {
                    connector.with("transforms", "schemaRename");
                    connector.with("transforms.schemaRename.type", "com.nryanov.kafka.connect.toolkit.debezium.transforms.SchemaRename");
                    connector.with("transforms.schemaRename.name", "new_schema_name.new_value_name");
                }
        );

        debeziumHelper.executeSql("INSERT INTO public.data(value) VALUES('some-data')");

        var msg = debeziumHelper.readAvroMessages("with-transformation.public.data", 1);
        assertEquals(1, msg.size());

        var schema = debeziumHelper.getLatestSchema("with-transformation.public.data-value");

        assertEquals("with_transformation.public.data.Envelope", schema.getFullName());
        assertEquals("new_schema_name.new_value_name", schema.getField("before").schema().getTypes().getLast().getFullName());
        assertEquals("new_schema_name.new_value_name", schema.getField("after").schema().getTypes().getLast().getFullName());
    }
}
