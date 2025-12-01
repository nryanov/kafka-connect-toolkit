package com.nryanov.kafka.connect.toolkit.fixtures.kafka.connect.helper;

import com.nryanov.kafka.connect.toolkit.fixtures.debezium.helper.DebeziumHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.connect.KafkaConnectFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaAdminHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaConsumerHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaProducerHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.model.RawMessage;
import com.nryanov.kafka.connect.toolkit.fixtures.postgres.PostgresFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.postgres.helper.PostgresHelper;
import io.debezium.testing.testcontainers.ConnectorConfiguration;

import java.time.Duration;
import java.util.List;

public class KafkaConnectDebeziumHelper {
    private final static String SLOT = "debezium_slot";
    private final static String PUBLICATION = "debezium_publication";
    private final static int DEFAULT_CONSUMER_TIMEOUT = 5;

    private final KafkaAdminHelper adminHelper;
    private final KafkaConsumerHelper consumerHelper;
    private final KafkaProducerHelper producerHelper;

    private final PostgresHelper postgresHelper;

    private final DebeziumHelper debeziumHelper;

    private final PostgresFixtureContainer postgres;

    public KafkaConnectDebeziumHelper(KafkaConnectFixtureContainer kafkaConnect) {
        this.adminHelper = new KafkaAdminHelper(kafkaConnect.getKafka());
        this.consumerHelper = new KafkaConsumerHelper(kafkaConnect.getKafka());
        this.producerHelper = new KafkaProducerHelper(kafkaConnect.getKafka());

        this.postgres = kafkaConnect.getPostgres();
        this.postgresHelper = new PostgresHelper(kafkaConnect.getPostgres());

        this.debeziumHelper = new DebeziumHelper(kafkaConnect.getDebezium());
    }

    public void createTopic(String topic) {
        adminHelper.createTopic(topic);
    }

    public void setupPostgresDebeziumConnector() {
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

    public boolean isConnectorRunning() {
        return debeziumHelper.isConnectorRunning("test");
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

    public List<RawMessage> readMessages(String topic, int limit) {
        return consumerHelper.read(topic, limit, Duration.ofSeconds(DEFAULT_CONSUMER_TIMEOUT));
    }

    public List<String> topics() {
        return adminHelper.topics();
    }
}
