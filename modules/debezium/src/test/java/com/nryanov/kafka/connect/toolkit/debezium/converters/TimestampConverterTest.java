package com.nryanov.kafka.connect.toolkit.debezium.converters;

import com.nryanov.kafka.connect.toolkit.fixtures.kafka_connect.KafkaConnectFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka_connect.helper.KafkaConnectDebeziumHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

    @Disabled("This is just a list of reference types and values generated without converter")
    @ParameterizedTest
    @CsvSource(delimiter = ',', quoteCharacter = '*', textBlock = """
            # CASE                                            JDBC_TYPE                                                           VALUE                       EXPECTED_VALUE                    EXPECTED_AVRO_TYPE
            reference_optional_timestamp_no_tz,               *TIMESTAMP WITHOUT TIME ZONE*,                                      *'2025-01-01 12:00:00'*,    *1735732800000000*,               *["null",{"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTimestamp"}]*
            reference_optional_timestamp_no_tz_with_default,  *TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP*,            *'2025-01-01 12:00:00'*,    *1735732800000000*,               *[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTimestamp"},"null"]*
            reference_required_timestamp_no_tz,               *TIMESTAMP WITHOUT TIME ZONE NOT NULL*,                             *'2025-01-01 12:00:00'*,    *1735732800000000*,               *{"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTimestamp"}*
            reference_required_timestamp_no_tz_with_default,  *TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,   *'2025-01-01 12:00:00'*,    *1735732800000000*,               *{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTimestamp"}*
            reference_optional_timestamp_tz,                  *TIMESTAMP WITH TIME ZONE*,                                         *'2025-01-01 12:00:00'*,    *2025-01-01T09:00:00.000000Z*,    *["null",{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTimestamp"}]*
            reference_optional_timestamp_tz_with_default,     *TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP*,               *'2025-01-01 12:00:00'*,    *2025-01-01T09:00:00.000000Z*,    *[{"type":"string","connect.version":1,"connect.default":"1970-01-01T00:00:00.000000Z","connect.name":"io.debezium.time.ZonedTimestamp"},"null"]*
            reference_required_timestamp_tz,                  *TIMESTAMP WITH TIME ZONE NOT NULL*,                                *'2025-01-01 12:00:00'*,    *2025-01-01T09:00:00.000000Z*,    *{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTimestamp"}*
            reference_required_timestamp_tz_with_default,     *TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,      *'2025-01-01 12:00:00'*,    *2025-01-01T09:00:00.000000Z*,    *{"type":"string","connect.version":1,"connect.default":"1970-01-01T00:00:00.000000Z","connect.name":"io.debezium.time.ZonedTimestamp"}*
            reference_optional_date,                          *DATE*,                                                             *'2025-01-01'*,             *20089*,                          *["null",{"type":"int","connect.version":1,"connect.name":"io.debezium.time.Date"}]*
            reference_optional_date_with_default,             *DATE DEFAULT NOW()*,                                               *'2025-01-01'*,             *20089*,                          *[{"type":"int","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Date"},"null"]*
            reference_required_date,                          *DATE NOT NULL*,                                                    *'2025-01-01'*,             *20089*,                          *{"type":"int","connect.version":1,"connect.name":"io.debezium.time.Date"}*
            reference_required_date_with_default,             *DATE NOT NULL DEFAULT NOW()*,                                      *'2025-01-01'*,             *20089*,                          *{"type":"int","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Date"}*
            reference_optional_time_no_tz,                    *TIME WITHOUT TIME ZONE*,                                           *'12:00:00'*,               *43200000000*,                    *["null",{"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTime"}]*
            reference_optional_time_no_tz_with_default,       *TIME WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP*,                 *'12:00:00'*,               *43200000000*,                    *[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTime"},"null"]*
            reference_required_time_no_tz,                    *TIME WITHOUT TIME ZONE NOT NULL*,                                  *'12:00:00'*,               *43200000000*,                    *{"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTime"}*
            reference_required_time_no_tz_with_default,       *TIME WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,        *'12:00:00'*,               *43200000000*,                    *{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTime"}*
            reference_optional_time_tz,                       *TIME WITH TIME ZONE*,                                              *'12:00:00'*,               *09:00:00Z*,                      *["null",{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTime"}]*
            reference_optional_time_tz_with_default,          *TIME WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP*,                    *'12:00:00'*,               *09:00:00Z*,                      *["null",{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTime"}]*
            reference_required_time_tz,                       *TIME WITH TIME ZONE NOT NULL*,                                     *'12:00:00'*,               *09:00:00Z*,                      *{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTime"}*
            reference_required_time_tz_with_default,          *TIME WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,           *'12:00:00'*,               *09:00:00Z*,                      *{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTime"}*
            """)
    public void referenceTypesAndValues(
            String testCase,
            String jdbcType,
            String inputValue,
            String expectedValue,
            String expectedAvroType
    ) {
        var topicPrefix = testCase;
        var publication = testCase;
        var table = String.format("public.%s", testCase);
        var topic = String.format("%s.%s", topicPrefix, table);
        var subject = String.format("%s.%s-value", topicPrefix, table);

        setupPublication(testCase, table, jdbcType, publication);
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, publication, topicPrefix, ignored -> {});
        insertData(table, inputValue);

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        var schema = debeziumHelper.getLatestSchema(subject);

        assertEquals(expectedValue, msg.getFirst().getNestedRecord("after").get("value").toString());
        assertEquals(expectedAvroType, schema.after().getField("value").schema().toString());
    }

    @ParameterizedTest
    @CsvSource(delimiter = ',', quoteCharacter = '*', textBlock = """
            # CASE                                  JDBC_TYPE                                                           VALUE                       EXPECTED_VALUE                  EXPECTED_AVRO_TYPE                                                          SNAPSHOT
            optional_timestamp_no_tz,               *TIMESTAMP WITHOUT TIME ZONE*,                                      *'2025-01-01 12:00:00'*,    *2025-01-01T12:00:00.000*,      *["null","string"]*,                                                        NONE
            optional_timestamp_no_tz_with_default,  *TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP*,            *'2025-01-01 12:00:00'*,    *2025-01-01T12:00:00.000*,      *[{"type":"string","connect.default":"1970-01-01T00:00:00.000"},"null"]*,   NONE
            required_timestamp_no_tz,               *TIMESTAMP WITHOUT TIME ZONE NOT NULL*,                             *'2025-01-01 12:00:00'*,    *2025-01-01T12:00:00.000*,      *["null","string"]*,                                                        NONE
            required_timestamp_no_tz_with_default,  *TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,   *'2025-01-01 12:00:00'*,    *2025-01-01T12:00:00.000*,      *[{"type":"string","connect.default":"1970-01-01T00:00:00.000"},"null"]*,   NONE
            optional_timestamp_no_tz_blocking,      *TIMESTAMP WITHOUT TIME ZONE*,                                      *'2025-01-01 12:00:00'*,    *2025-01-01T12:00:00.000*,      *["null","string"]*,                                                        BLOCKING
            optional_timestamp_no_tz_incremental,   *TIMESTAMP WITHOUT TIME ZONE*,                                      *'2025-01-01 12:00:00'*,    *2025-01-01T12:00:00.000*,      *["null","string"]*,                                                        INCREMENTAL
            """)
    public void convertTimestampWithoutTimezoneUsingConverter(
            String testCase,
            String jdbcType,
            String inputValue,
            String expectedValue,
            String expectedAvroType,
            String snapshot
    ) {
        var topicPrefix = testCase;
        var publication = testCase;
        var table = String.format("public.%s", testCase);
        var topic = String.format("%s.%s", topicPrefix, table);
        var subject = String.format("%s.%s-value", topicPrefix, table);

        setupPublication(testCase, table, jdbcType, publication);
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, publication, topicPrefix, connector -> {
            connector.with("converters", "timestampConverter");
            connector.with("timestampConverter.type", "com.nryanov.kafka.connect.toolkit.debezium.converters.TimestampConverter");
        });
        insertData(table, inputValue);
        conditionallyTriggerSnapshot(snapshot, topic, table);

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        var schema = debeziumHelper.getLatestSchema(subject);

        assertEquals(expectedValue, msg.getFirst().getNestedRecord("after").get("value").toString());
        assertEquals(expectedAvroType, schema.after().getField("value").schema().toString());
    }

    @ParameterizedTest
    @CsvSource(delimiter = ',', quoteCharacter = '*', textBlock = """
            # CASE                                  JDBC_TYPE                                                           VALUE                       EXPECTED_VALUE                  EXPECTED_AVRO_TYPE                                                          SNAPSHOT
            optional_timestamp_no_tz,               *TIMESTAMP WITHOUT TIME ZONE*,                                      *'2025-01-01 12:00:00'*,    *2025-01-01T18:00:00.000*,      *["null","string"]*,                                                        NONE
            optional_timestamp_no_tz_with_default,  *TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP*,            *'2025-01-01 14:00:00'*,    *2025-01-01T20:00:00.000*,      *[{"type":"string","connect.default":"1970-01-01T06:00:00.000"},"null"]*,   NONE
            required_timestamp_no_tz,               *TIMESTAMP WITHOUT TIME ZONE NOT NULL*,                             *'2025-01-01 16:00:00'*,    *2025-01-01T22:00:00.000*,      *["null","string"]*,                                                        NONE
            required_timestamp_no_tz_with_default,  *TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,   *'2025-01-01 18:00:00'*,    *2025-01-02T00:00:00.000*,      *[{"type":"string","connect.default":"1970-01-01T06:00:00.000"},"null"]*,   NONE
            optional_timestamp_no_tz_blocking,      *TIMESTAMP WITHOUT TIME ZONE*,                                      *'2025-01-01 20:00:00'*,    *2025-01-02T02:00:00.000*,      *["null","string"]*,                                                        BLOCKING
            optional_timestamp_no_tz_incremental,   *TIMESTAMP WITHOUT TIME ZONE*,                                      *'2025-01-01 22:00:00'*,    *2025-01-02T04:00:00.000*,      *["null","string"]*,                                                        INCREMENTAL
            """)
    public void convertTimestampWithoutTimezoneWithShiftUsingConverter(
            String testCase,
            String jdbcType,
            String inputValue,
            String expectedValue,
            String expectedAvroType,
            String snapshot
    ) {
        var topicPrefix = testCase;
        var publication = testCase;
        var table = String.format("public.%s", testCase);
        var topic = String.format("%s.%s", topicPrefix, table);
        var subject = String.format("%s.%s-value", topicPrefix, table);

        setupPublication(testCase, table, jdbcType, publication);
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, publication, topicPrefix, connector -> {
            connector.with("converters", "timestampConverter");
            connector.with("timestampConverter.type", "com.nryanov.kafka.connect.toolkit.debezium.converters.TimestampConverter");
            connector.with("timestampConverter.timestamp.shift", "+06:00");
        });
        insertData(table, inputValue);
        conditionallyTriggerSnapshot(snapshot, topic, table);

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        var schema = debeziumHelper.getLatestSchema(subject);

        assertEquals(expectedValue, msg.getFirst().getNestedRecord("after").get("value").toString());
        assertEquals(expectedAvroType, schema.after().getField("value").schema().toString());
    }

    @ParameterizedTest
    @CsvSource(delimiter = ',', quoteCharacter = '*', textBlock = """
            # CASE                                  JDBC_TYPE                                                           VALUE                       EXPECTED_VALUE                  EXPECTED_AVRO_TYPE                                                                                                                                              SNAPSHOT
            optional_timestamp_tz,                  *TIMESTAMP WITH TIME ZONE*,                                         *'2025-01-01 12:00:00'*,    *1735722000000*,                *["null",{"type":"long","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Timestamp","logicalType":"timestamp-millis"}]*,                       NONE
            optional_timestamp_tz_with_default,     *TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP*,               *'2025-01-01 12:00:00'*,    *1735722000000*,                *[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"org.apache.kafka.connect.data.Timestamp","logicalType":"timestamp-millis"},"null"]*,   NONE
            required_timestamp_tz,                  *TIMESTAMP WITH TIME ZONE NOT NULL*,                                *'2025-01-01 12:00:00'*,    *1735722000000*,                *["null",{"type":"long","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Timestamp","logicalType":"timestamp-millis"}]*,                       NONE
            required_timestamp_tz_with_default,     *TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,      *'2025-01-01 12:00:00'*,    *1735722000000*,                *[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"org.apache.kafka.connect.data.Timestamp","logicalType":"timestamp-millis"},"null"]*,   NONE
            optional_timestamp_tz_blocking,         *TIMESTAMP WITH TIME ZONE*,                                         *'2025-01-01 12:00:00'*,    *1735722000000*,                *["null",{"type":"long","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Timestamp","logicalType":"timestamp-millis"}]*,                       BLOCKING
            optional_timestamp_tz_incremental,      *TIMESTAMP WITH TIME ZONE*,                                         *'2025-01-01 12:00:00'*,    *1735722000000*,                *["null",{"type":"long","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Timestamp","logicalType":"timestamp-millis"}]*,                       INCREMENTAL
            """)
    public void convertTimestampWithTimezoneUsingConverter(
            String testCase,
            String jdbcType,
            String inputValue,
            String expectedValue,
            String expectedAvroType,
            String snapshot
    ) {
        var topicPrefix = testCase;
        var publication = testCase;
        var table = String.format("public.%s", testCase);
        var topic = String.format("%s.%s", topicPrefix, table);
        var subject = String.format("%s.%s-value", topicPrefix, table);

        setupPublication(testCase, table, jdbcType, publication);
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, publication, topicPrefix, connector -> {
            connector.with("converters", "timestampConverter");
            connector.with("timestampConverter.type", "com.nryanov.kafka.connect.toolkit.debezium.converters.TimestampConverter");
        });
        insertData(table, inputValue);
        conditionallyTriggerSnapshot(snapshot, topic, table);

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        var schema = debeziumHelper.getLatestSchema(subject);

        assertEquals(expectedValue, msg.getFirst().getNestedRecord("after").get("value").toString());
        assertEquals(expectedAvroType, schema.after().getField("value").schema().toString());
    }

    @ParameterizedTest
    @CsvSource(delimiter = ',', quoteCharacter = '*', textBlock = """
            # CASE                                  JDBC_TYPE                                                           VALUE                       EXPECTED_VALUE                  EXPECTED_AVRO_TYPE                                                                                                                          SNAPSHOT
            optional_date,                          *DATE*,                                                             *'2025-01-01'*,             *20089*,                        *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Date","logicalType":"date"}]*,                     NONE
            optional_date_with_default,             *DATE DEFAULT NOW()*,                                               *'2025-01-01'*,             *20089*,                        *[{"type":"int","connect.version":1,"connect.default":0,"connect.name":"org.apache.kafka.connect.data.Date","logicalType":"date"},"null"]*, NONE
            required_date,                          *DATE NOT NULL*,                                                    *'2025-01-01'*,             *20089*,                        *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Date","logicalType":"date"}]*,                     NONE
            required_date_with_default,             *DATE NOT NULL DEFAULT NOW()*,                                      *'2025-01-01'*,             *20089*,                        *[{"type":"int","connect.version":1,"connect.default":0,"connect.name":"org.apache.kafka.connect.data.Date","logicalType":"date"},"null"]*, NONE
            optional_date_blocking,                 *DATE*,                                                             *'2025-01-01'*,             *20089*,                        *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Date","logicalType":"date"}]*,                     BLOCKING
            optional_date_incremental,              *DATE*,                                                             *'2025-01-01'*,             *20089*,                        *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Date","logicalType":"date"}]*,                     INCREMENTAL
            """)
    public void convertDateUsingConverter(
            String testCase,
            String jdbcType,
            String inputValue,
            String expectedValue,
            String expectedAvroType,
            String snapshot
    ) {
        var topicPrefix = testCase;
        var publication = testCase;
        var table = String.format("public.%s", testCase);
        var topic = String.format("%s.%s", topicPrefix, table);
        var subject = String.format("%s.%s-value", topicPrefix, table);

        setupPublication(testCase, table, jdbcType, publication);
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, publication, topicPrefix, connector -> {
            connector.with("converters", "timestampConverter");
            connector.with("timestampConverter.type", "com.nryanov.kafka.connect.toolkit.debezium.converters.TimestampConverter");
        });
        insertData(table, inputValue);
        conditionallyTriggerSnapshot(snapshot, topic, table);

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        var schema = debeziumHelper.getLatestSchema(subject);

        assertEquals(expectedValue, msg.getFirst().getNestedRecord("after").get("value").toString());
        assertEquals(expectedAvroType, schema.after().getField("value").schema().toString());
    }

    @ParameterizedTest
    @CsvSource(delimiter = ',', quoteCharacter = '*', textBlock = """
            # CASE                                  JDBC_TYPE                                                           VALUE                       EXPECTED_VALUE                  EXPECTED_AVRO_TYPE                                              SNAPSHOT
            optional_time_no_tz,                    *TIME WITHOUT TIME ZONE*,                                           *'12:00:00'*,               *12:00:00.000*,                 *["null","string"]*,                                            NONE
            optional_time_no_tz_with_default,       *TIME WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP*,                 *'12:00:00'*,               *12:00:00.000*,                 *[{"type":"string","connect.default":"00:00:00.000"},"null"]*,  NONE
            required_time_no_tz,                    *TIME WITHOUT TIME ZONE NOT NULL*,                                  *'12:00:00'*,               *12:00:00.000*,                 *["null","string"]*,                                            NONE
            required_time_no_tz_with_default,       *TIME WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,        *'12:00:00'*,               *12:00:00.000*,                 *[{"type":"string","connect.default":"00:00:00.000"},"null"]*,  NONE
            optional_time_no_tz_blocking,           *TIME WITHOUT TIME ZONE*,                                           *'12:00:00'*,               *12:00:00.000*,                 *["null","string"]*,                                            BLOCKING
            optional_time_no_tz_incremental,        *TIME WITHOUT TIME ZONE*,                                           *'12:00:00'*,               *12:00:00.000*,                 *["null","string"]*,                                            INCREMENTAL
            """)
    public void convertTimeWithoutTimezoneConverter(
            String testCase,
            String jdbcType,
            String inputValue,
            String expectedValue,
            String expectedAvroType,
            String snapshot
    ) {
        var topicPrefix = testCase;
        var publication = testCase;
        var table = String.format("public.%s", testCase);
        var topic = String.format("%s.%s", topicPrefix, table);
        var subject = String.format("%s.%s-value", topicPrefix, table);

        setupPublication(testCase, table, jdbcType, publication);
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, publication, topicPrefix, connector -> {
            connector.with("converters", "timestampConverter");
            connector.with("timestampConverter.type", "com.nryanov.kafka.connect.toolkit.debezium.converters.TimestampConverter");
        });
        insertData(table, inputValue);
        conditionallyTriggerSnapshot(snapshot, topic, table);

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        var schema = debeziumHelper.getLatestSchema(subject);

        assertEquals(expectedValue, msg.getFirst().getNestedRecord("after").get("value").toString());
        assertEquals(expectedAvroType, schema.after().getField("value").schema().toString());
    }

    @ParameterizedTest
    @CsvSource(delimiter = ',', quoteCharacter = '*', textBlock = """
            # CASE                                  JDBC_TYPE                                                           VALUE                       EXPECTED_VALUE                  EXPECTED_AVRO_TYPE                                              SNAPSHOT
            optional_time_no_tz,                    *TIME WITHOUT TIME ZONE*,                                           *'12:00:00'*,               *08:00:00.000*,                 *["null","string"]*,                                            NONE
            optional_time_no_tz_with_default,       *TIME WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP*,                 *'10:00:00'*,               *06:00:00.000*,                 *[{"type":"string","connect.default":"20:00:00.000"},"null"]*,  NONE
            required_time_no_tz,                    *TIME WITHOUT TIME ZONE NOT NULL*,                                  *'08:00:00'*,               *04:00:00.000*,                 *["null","string"]*,                                            NONE
            required_time_no_tz_with_default,       *TIME WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,        *'06:00:00'*,               *02:00:00.000*,                 *[{"type":"string","connect.default":"20:00:00.000"},"null"]*,  NONE
            optional_time_no_tz_blocking,           *TIME WITHOUT TIME ZONE*,                                           *'04:00:00'*,               *00:00:00.000*,                 *["null","string"]*,                                            BLOCKING
            optional_time_no_tz_incremental,        *TIME WITHOUT TIME ZONE*,                                           *'02:00:00'*,               *22:00:00.000*,                 *["null","string"]*,                                            INCREMENTAL
            """)
    public void convertTimeWithoutTimezoneWithShiftConverter(
            String testCase,
            String jdbcType,
            String inputValue,
            String expectedValue,
            String expectedAvroType,
            String snapshot
    ) {
        var topicPrefix = testCase;
        var publication = testCase;
        var table = String.format("public.%s", testCase);
        var topic = String.format("%s.%s", topicPrefix, table);
        var subject = String.format("%s.%s-value", topicPrefix, table);

        setupPublication(testCase, table, jdbcType, publication);
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, publication, topicPrefix, connector -> {
            connector.with("converters", "timestampConverter");
            connector.with("timestampConverter.type", "com.nryanov.kafka.connect.toolkit.debezium.converters.TimestampConverter");
            connector.with("timestampConverter.time.shift", "-04:00");
        });
        insertData(table, inputValue);
        conditionallyTriggerSnapshot(snapshot, topic, table);

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        var schema = debeziumHelper.getLatestSchema(subject);

        assertEquals(expectedValue, msg.getFirst().getNestedRecord("after").get("value").toString());
        assertEquals(expectedAvroType, schema.after().getField("value").schema().toString());
    }

    @ParameterizedTest
    @CsvSource(delimiter = ',', quoteCharacter = '*', textBlock = """
            # CASE                                  JDBC_TYPE                                                           VALUE                       EXPECTED_VALUE                  EXPECTED_AVRO_TYPE                                                                                                              SNAPSHOT
            optional_time_tz,                       *TIME WITH TIME ZONE*,                                              *'12:00:00'*,               *32400000*,                     *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Time","logicalType":"time-millis"}]*,  NONE
            optional_time_tz_with_default,          *TIME WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP*,                    *'12:00:00'*,               *32400000*,                     *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Time","logicalType":"time-millis"}]*,  NONE
            required_time_tz,                       *TIME WITH TIME ZONE NOT NULL*,                                     *'12:00:00'*,               *32400000*,                     *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Time","logicalType":"time-millis"}]*,  NONE
            required_time_tz_with_default,          *TIME WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,           *'12:00:00'*,               *32400000*,                     *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Time","logicalType":"time-millis"}]*,  NONE
            optional_time_tz_blocking,              *TIME WITH TIME ZONE*,                                              *'12:00:00'*,               *32400000*,                     *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Time","logicalType":"time-millis"}]*,  BLOCKING
            optional_time_tz_incremental,           *TIME WITH TIME ZONE*,                                              *'12:00:00'*,               *32400000*,                     *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Time","logicalType":"time-millis"}]*,  INCREMENTAL
            """)
    public void convertTimeWithTimezoneUsingConverter(
            String testCase,
            String jdbcType,
            String inputValue,
            String expectedValue,
            String expectedAvroType,
            String snapshot
    ) {
        var topicPrefix = testCase;
        var publication = testCase;
        var table = String.format("public.%s", testCase);
        var topic = String.format("%s.%s", topicPrefix, table);
        var subject = String.format("%s.%s-value", topicPrefix, table);

        setupPublication(testCase, table, jdbcType, publication);
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, publication, topicPrefix, connector -> {
            connector.with("converters", "timestampConverter");
            connector.with("timestampConverter.type", "com.nryanov.kafka.connect.toolkit.debezium.converters.TimestampConverter");
        });
        insertData(table, inputValue);
        conditionallyTriggerSnapshot(snapshot, topic, table);

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        var schema = debeziumHelper.getLatestSchema(subject);

        assertEquals(expectedValue, msg.getFirst().getNestedRecord("after").get("value").toString());
        assertEquals(expectedAvroType, schema.after().getField("value").schema().toString());
    }

    private void setupPublication(String testCase, String table, String jdbcType, String publication) {
        debeziumHelper.setupPublication(testCase);
        debeziumHelper.executeSql(String.format("CREATE TABLE IF NOT EXISTS %s (id BIGSERIAL PRIMARY KEY, value %s)", table, jdbcType));
        debeziumHelper.addTableToPublication(publication, table);
    }

    private void conditionallyTriggerSnapshot(String snapshot, String topic, String table) {
        if (!"NONE".equals(snapshot)) {
            // skip first event
            debeziumHelper.readAvroMessages(topic, 1);
            debeziumHelper.executeSql(String.format("INSERT INTO public.signals(id, type, data) VALUES (gen_random_uuid(), 'execute-snapshot', '{\"type\": \"%s\", \"data-collections\": [\"%s\"]}')", snapshot, table));
        }
    }

    private void insertData(String table, String inputValue) {
        debeziumHelper.executeSql(String.format("INSERT INTO %s(value) VALUES (%s)", table, inputValue));
    }
}
