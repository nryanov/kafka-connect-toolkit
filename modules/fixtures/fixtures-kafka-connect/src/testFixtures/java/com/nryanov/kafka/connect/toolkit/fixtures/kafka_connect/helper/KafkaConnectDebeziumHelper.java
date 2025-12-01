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

import java.time.Duration;
import java.util.List;

public class KafkaConnectDebeziumHelper {
    private final static String SLOT = "debezium_slot";
    private final static String PUBLICATION = "debezium_publication";
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

    public void setupPostgresDebeziumConnectorJsonFormat() {
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
                .with("publication.name", PUBLICATION)
                .with("slot.name", SLOT)
                .with("plugin.name", "pgoutput")
                .with("snapshot.mode", "NO_DATA")
                .with("topic.prefix", "prefix");

        debeziumHelper.registerConnector("test", connector);
    }

    public void setupPostgresDebeziumConnectorAvroFormat() {
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
                .with("publication.name", PUBLICATION)
                .with("slot.name", SLOT)
                .with("plugin.name", "pgoutput")
                .with("snapshot.mode", "NO_DATA")
                .with("topic.prefix", "prefix")

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

        debeziumHelper.registerConnector("test", connector);
    }

    public boolean isConnectorRunning(String name) {
        return debeziumHelper.isConnectorRunning(name);
    }

    public boolean isConnectorTaskRunning(String name) {
        return debeziumHelper.isConnectorTaskRunning(name, 0);
    }

    public void setupLogicalReplication() {
        postgresHelper.inTransaction(tx -> {
            tx.execute("SELECT PG_CREATE_LOGICAL_REPLICATION_SLOT(?, ?)", SLOT, "pgoutput");
            tx.execute("CREATE PUBLICATION debezium_publication");

            tx.execute("CREATE TABLE heartbeat(single_row BOOL PRIMARY KEY DEFAULT TRUE, \"timestamp\" TIMESTAMP NOT NULL, CONSTRAINT single_row_check CHECK (single_row))");
            tx.execute("CREATE TABLE signals(id TEXT PRIMARY KEY, type TEXT NOT NULL, data TEXT)");
            tx.execute("CREATE TABLE offsets(id TEXT PRIMARY KEY, offset_key TEXT, offset_val TEXT, record_insert_ts TIMESTAMP NOT NULL, record_insert_seq INTEGER NOt NULL)");

            tx.execute("ALTER PUBLICATION debezium_publication ADD TABLE heartbeat");
            tx.execute("ALTER PUBLICATION debezium_publication ADD TABLE signals");

            return 0;
        });
    }

    public void addTableToPublication(String table) {
        postgresHelper.inTransaction(tx -> {
            tx.execute("ALTER PUBLICATION debezium_publication ADD TABLE " + table);

            return 0;
        });
    }

    public void removeTableFromPublication(String table) {
        postgresHelper.inTransaction(tx -> {
            tx.execute("ALTER PUBLICATION debezium_publication DROP TABLE " + table);

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
}
