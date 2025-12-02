package com.nryanov.kafka.connect.toolkit.fixtures.kafka_connect.helper;

import com.nryanov.kafka.connect.toolkit.fixtures.debezium.helper.DebeziumHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.KafkaFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaAvroConsumerHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaAvroProducerHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaRawConsumerHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaRawProducerHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka_connect.KafkaConnectFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaAdminHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.model.AvroMessage;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.model.RawMessage;
import com.nryanov.kafka.connect.toolkit.fixtures.postgres.PostgresFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.postgres.helper.PostgresHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.schema_registry.SchemaRegistryFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.schema_registry.helper.SchemaRegistryHelper;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import org.apache.avro.Schema;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

public class KafkaConnectDebeziumHelper {
    private final static String SLOT = "debezium_slot";
    private final static int DEFAULT_CONSUMER_TIMEOUT = 5;

    private final KafkaAdminHelper adminHelper;
    private final KafkaRawConsumerHelper rawConsumerHelper;
    private final KafkaRawProducerHelper rawProducerHelper;
    private final KafkaAvroConsumerHelper avroConsumerHelper;
    private final KafkaAvroProducerHelper avroProducerHelper;
    private final SchemaRegistryHelper schemaRegistryHelper;

    private final PostgresHelper postgresHelper;

    private final DebeziumHelper debeziumHelper;

    private final KafkaFixtureContainer kafka;
    private final SchemaRegistryFixtureContainer schemaRegistry;
    private final PostgresFixtureContainer postgres;

    public KafkaConnectDebeziumHelper(KafkaConnectFixtureContainer kafkaConnect) {
        this.adminHelper = new KafkaAdminHelper(kafkaConnect.getKafka());
        this.rawConsumerHelper = new KafkaRawConsumerHelper(kafkaConnect.getKafka());
        this.rawProducerHelper = new KafkaRawProducerHelper(kafkaConnect.getKafka());
        this.avroConsumerHelper = new KafkaAvroConsumerHelper(kafkaConnect.getKafka(), kafkaConnect.getSchemaRegistry());
        this.avroProducerHelper = new KafkaAvroProducerHelper(kafkaConnect.getKafka(), kafkaConnect.getSchemaRegistry());
        this.schemaRegistryHelper = new SchemaRegistryHelper(kafkaConnect.getSchemaRegistry());

        this.kafka = kafkaConnect.getKafka();
        this.schemaRegistry = kafkaConnect.getSchemaRegistry();

        this.postgres = kafkaConnect.getPostgres();
        this.postgresHelper = new PostgresHelper(kafkaConnect.getPostgres());

        this.debeziumHelper = new DebeziumHelper(kafkaConnect.getDebezium());
    }

    public void createTopic(String topic) {
        adminHelper.createTopic(topic);
    }

    public void setupPostgresDebeziumConnectorJsonFormat(
            String connectorName,
            String topicPrefix,
            String publicationName,
            Consumer<ConnectorConfiguration> code
    ) {
        var connector = ConnectorConfiguration.create()
//                .with("heartbeat.interval.ms", "1000")
//                .with("heartbeat.action.query", "INSERT INTO heartbeat(last_update) VALUES (current_timestamp) ON CONFLICT (single_row) DO UPDATE SET last_update=current_timestamp")
                .with("publication.autocreate.mode", "disabled")
                .with("database.query.timeout.ms", "0")
                .with("tombstones.on.delete", "false")
                .with("time.precision.mode", "adaptive")
                .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .with("offset.storage", "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore")

                .with("offset.flush.interval.ms", "5000")
                .with("offset.storage.jdbc.connection.url", postgres.networkJdbcUrl())
                .with("offset.storage.jdbc.connection.user", postgres.username())
                .with("offset.storage.jdbc.connection.password", postgres.password())
                .with("offset.storage.jdbc.table.name", "public.offsets")

                .with("signal.enabled.channels", "source")
                .with("signal.data.collection", "public.signals")
                .with("max.batch.size", "100")
                .with("max.queue.size", "1024")
                .with("poll.interval.ms", "500")

                .with("database.hostname", postgres.networkHost())
                .with("database.dbname", postgres.database())
                .with("database.port", postgres.networkPort())
                .with("database.user", postgres.username())
                .with("database.password", postgres.password())
                .with("publication.name", publicationName)
                .with("slot.name", SLOT)
                .with("plugin.name", "pgoutput")
                .with("snapshot.mode", "NO_DATA")
                .with("topic.prefix", topicPrefix);

        code.accept(connector);

        debeziumHelper.registerConnector(connectorName, connector);
    }

    public void setupPostgresDebeziumConnectorAvroFormat(
            String connectorName,
            String topicPrefix,
            String publicationName,
            Consumer<ConnectorConfiguration> code
    ) {
        var connector = ConnectorConfiguration.create()
//                .with("heartbeat.interval.ms", "1000")
//                .with("heartbeat.action.query", "INSERT INTO heartbeat(last_update) VALUES (current_timestamp) ON CONFLICT (single_row) DO UPDATE SET last_update=current_timestamp")
                .with("publication.autocreate.mode", "disabled")
                .with("database.query.timeout.ms", "0")
                .with("tombstones.on.delete", "false")
                .with("time.precision.mode", "adaptive")
                .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .with("offset.storage", "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore")

                .with("offset.flush.interval.ms", "5000")
                .with("offset.storage.jdbc.connection.url", postgres.networkJdbcUrl())
                .with("offset.storage.jdbc.connection.user", postgres.username())
                .with("offset.storage.jdbc.connection.password", postgres.password())
                .with("offset.storage.jdbc.table.name", "public.offsets")

                .with("signal.enabled.channels", "source")
                .with("signal.data.collection", "public.signals")
                .with("max.batch.size", "100")
                .with("max.queue.size", "1024")
                .with("poll.interval.ms", "500")

                .with("database.hostname", postgres.networkHost())
                .with("database.dbname", postgres.database())
                .with("database.port", postgres.networkPort())
                .with("database.user", postgres.username())
                .with("database.password", postgres.password())
                .with("publication.name", publicationName)
                .with("slot.name", SLOT)
                .with("plugin.name", "pgoutput")
                .with("snapshot.mode", "NO_DATA")
                .with("topic.prefix", topicPrefix)

                .with("key.converter", "io.apicurio.registry.utils.converter.AvroConverter")
                .with("key.converter.apicurio.registry.url", schemaRegistry.networkUrl())
                .with("key.converter.apicurio.registry.auto-register", "true")
                .with("key.converter.apicurio.registry.find-latest", "false")
                .with("key.converter.schemas.enable", "false")
                .with("key.converter.apicurio.registry.headers.enabled", "false")
                .with("key.converter.apicurio.registry.as-confluent", "true")
                .with("key.converter.apicurio.use-id", "contentId")

                .with("value.converter", "io.apicurio.registry.utils.converter.AvroConverter")
                .with("value.converter.apicurio.registry.url", schemaRegistry.networkUrl())
                .with("value.converter.apicurio.registry.auto-register", "true")
                .with("value.converter.apicurio.registry.find-latest", "false")
                .with("value.converter.schemas.enable", "false")
                .with("value.converter.apicurio.registry.headers.enabled", "false")
                .with("value.converter.apicurio.registry.as-confluent", "true")
                .with("value.converter.apicurio.use-id", "contentId")

                .with("schema.name.adjustment.mode", "avro");

        code.accept(connector);

        debeziumHelper.registerConnector(connectorName, connector);
    }

    public void deleteConnector(String connector) {
        try {
        debeziumHelper.deleteConnector(connector);
        } catch (Exception e) {
            // ignore
        }
    }

    public boolean isConnectorRunning(String name) {
        return debeziumHelper.isConnectorRunning(name);
    }

    public boolean isConnectorTaskRunning(String name) {
        return debeziumHelper.isConnectorTaskRunning(name, 0);
    }

    public void setupLogicalReplicationSlot() {
        postgresHelper.inTransaction(tx -> {
            tx.execute("SELECT PG_CREATE_LOGICAL_REPLICATION_SLOT(?, ?)", SLOT, "pgoutput");
            return 0;
        });
    }

    public void dropLogicalReplicationSlot() {
        postgresHelper.inTransaction(tx -> {
            tx.execute("SELECT PG_DROP_REPLICATION_SLOT(?)", SLOT);
            return 0;
        });
    }

    public void setupSystemTables() {
        postgresHelper.inTransaction(tx -> {
            tx.execute("CREATE TABLE heartbeat(single_row BOOL PRIMARY KEY DEFAULT TRUE, \"timestamp\" TIMESTAMP NOT NULL, CONSTRAINT single_row_check CHECK (single_row))");
            tx.execute("CREATE TABLE signals(id TEXT PRIMARY KEY, type TEXT NOT NULL, data TEXT)");
            tx.execute("CREATE TABLE offsets(id TEXT PRIMARY KEY, offset_key TEXT, offset_val TEXT, record_insert_ts TIMESTAMP NOT NULL, record_insert_seq INTEGER NOt NULL)");

            return 0;
        });
    }

    public void setupPublication(String publicationName) {
        postgresHelper.inTransaction(tx -> {
            tx.execute(String.format("CREATE PUBLICATION %s", publicationName));
            addTableToPublication(publicationName, "public.heartbeat");
            addTableToPublication(publicationName, "public.signals");

            return 0;
        });
    }

    public void addTableToPublication(String publicationName, String table) {
        postgresHelper.inTransaction(tx -> {
            tx.execute(String.format("ALTER PUBLICATION %s ADD TABLE %s", publicationName, table));

            return 0;
        });
    }

    public void removeTableFromPublication(String publicationName, String table) {
        postgresHelper.inTransaction(tx -> {
            tx.execute(String.format("ALTER PUBLICATION %s DROP TABLE %s", publicationName, table));

            return 0;
        });
    }

    public void truncateTable(String table) {
        postgresHelper.inTransaction(tx -> {
            tx.execute("TRUNCATE " + table);

            return 0;
        });
    }

    public void executeSql(String sql) {
        postgresHelper.inTransaction(tx -> {
            tx.execute(sql);

            return 0;
        });
    }

    public List<RawMessage> readRawMessages(String topic, int limit) {
        return rawConsumerHelper.read(topic, limit, Duration.ofSeconds(DEFAULT_CONSUMER_TIMEOUT));
    }

    public List<AvroMessage> readAvroMessages(String topic, int limit) {
        return avroConsumerHelper.read(topic, limit, Duration.ofSeconds(DEFAULT_CONSUMER_TIMEOUT));
    }

    public List<String> topics() {
        return adminHelper.topics();
    }

    public List<String> getSubjects() {
        return schemaRegistryHelper.subjects();
    }

    public Schema getLatestSchema(String subject) {
        return schemaRegistryHelper.getLatestSchema(subject);
    }

    public Schema getSchema(String subject, int version) {
        return schemaRegistryHelper.getSubjectSchemaByVersions(subject, version);
    }

    public List<Integer> getSubjectVersions(String subject) {
        return schemaRegistryHelper.getSubjectVersions(subject);
    }
}
