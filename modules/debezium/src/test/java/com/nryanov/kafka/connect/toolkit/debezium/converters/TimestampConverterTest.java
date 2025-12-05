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

        debeziumHelper.setupPublication(testCase);
        debeziumHelper.executeSql(String.format("CREATE TABLE IF NOT EXISTS %s (id BIGSERIAL PRIMARY KEY, value %s)", table, jdbcType));
        debeziumHelper.addTableToPublication(publication, table);
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, publication, topicPrefix, ignored -> {});
        debeziumHelper.executeSql(String.format("INSERT INTO %s(value) VALUES (%s)", table, inputValue));

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        var schema = debeziumHelper.getLatestSchema(subject);

        assertEquals(expectedValue, msg.getFirst().getNestedRecord("after").get("value").toString());
        assertEquals(expectedAvroType, schema.after().getField("value").schema().toString());
    }

    @Disabled("This is just a list of reference types and values generated without converter during initial blocking snapshot")
    @ParameterizedTest
    @CsvSource(delimiter = ',', quoteCharacter = '*', textBlock = """
            # CASE                                                     JDBC_TYPE                                                           VALUE                       EXPECTED_VALUE                    EXPECTED_AVRO_TYPE
            reference_blocking_optional_timestamp_no_tz,               *TIMESTAMP WITHOUT TIME ZONE*,                                      *'2025-01-01 12:00:00'*,    *1735732800000000*,               *["null",{"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTimestamp"}]*
            reference_blocking_optional_timestamp_no_tz_with_default,  *TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP*,            *'2025-01-01 12:00:00'*,    *1735732800000000*,               *[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTimestamp"},"null"]*
            reference_blocking_required_timestamp_no_tz,               *TIMESTAMP WITHOUT TIME ZONE NOT NULL*,                             *'2025-01-01 12:00:00'*,    *1735732800000000*,               *{"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTimestamp"}*
            reference_blocking_required_timestamp_no_tz_with_default,  *TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,   *'2025-01-01 12:00:00'*,    *1735732800000000*,               *{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTimestamp"}*
            reference_blocking_optional_timestamp_tz,                  *TIMESTAMP WITH TIME ZONE*,                                         *'2025-01-01 12:00:00'*,    *2025-01-01T09:00:00.000000Z*,    *["null",{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTimestamp"}]*
            reference_blocking_optional_timestamp_tz_with_default,     *TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP*,               *'2025-01-01 12:00:00'*,    *2025-01-01T09:00:00.000000Z*,    *[{"type":"string","connect.version":1,"connect.default":"1970-01-01T00:00:00.000000Z","connect.name":"io.debezium.time.ZonedTimestamp"},"null"]*
            reference_blocking_required_timestamp_tz,                  *TIMESTAMP WITH TIME ZONE NOT NULL*,                                *'2025-01-01 12:00:00'*,    *2025-01-01T09:00:00.000000Z*,    *{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTimestamp"}*
            reference_blocking_required_timestamp_tz_with_default,     *TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,      *'2025-01-01 12:00:00'*,    *2025-01-01T09:00:00.000000Z*,    *{"type":"string","connect.version":1,"connect.default":"1970-01-01T00:00:00.000000Z","connect.name":"io.debezium.time.ZonedTimestamp"}*
            reference_blocking_optional_date,                          *DATE*,                                                             *'2025-01-01'*,             *20089*,                          *["null",{"type":"int","connect.version":1,"connect.name":"io.debezium.time.Date"}]*
            reference_blocking_optional_date_with_default,             *DATE DEFAULT NOW()*,                                               *'2025-01-01'*,             *20089*,                          *[{"type":"int","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Date"},"null"]*
            reference_blocking_required_date,                          *DATE NOT NULL*,                                                    *'2025-01-01'*,             *20089*,                          *{"type":"int","connect.version":1,"connect.name":"io.debezium.time.Date"}*
            reference_blocking_required_date_with_default,             *DATE NOT NULL DEFAULT NOW()*,                                      *'2025-01-01'*,             *20089*,                          *{"type":"int","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Date"}*
            reference_blocking_optional_time_no_tz,                    *TIME WITHOUT TIME ZONE*,                                           *'12:00:00'*,               *43200000000*,                    *["null",{"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTime"}]*
            reference_blocking_optional_time_no_tz_with_default,       *TIME WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP*,                 *'12:00:00'*,               *43200000000*,                    *[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTime"},"null"]*
            reference_blocking_required_time_no_tz,                    *TIME WITHOUT TIME ZONE NOT NULL*,                                  *'12:00:00'*,               *43200000000*,                    *{"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTime"}*
            reference_blocking_required_time_no_tz_with_default,       *TIME WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,        *'12:00:00'*,               *43200000000*,                    *{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTime"}*
            reference_blocking_optional_time_tz,                       *TIME WITH TIME ZONE*,                                              *'12:00:00'*,               *09:00:00Z*,                      *["null",{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTime"}]*
            reference_blocking_optional_time_tz_with_default,          *TIME WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP*,                    *'12:00:00'*,               *09:00:00Z*,                      *["null",{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTime"}]*
            reference_blocking_required_time_tz,                       *TIME WITH TIME ZONE NOT NULL*,                                     *'12:00:00'*,               *09:00:00Z*,                      *{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTime"}*
            reference_blocking_required_time_tz_with_default,          *TIME WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,           *'12:00:00'*,               *09:00:00Z*,                      *{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTime"}*
            """)
    public void referenceTypesAndValuesForBlockingSnapshot(
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

        debeziumHelper.setupPublication(testCase);
        debeziumHelper.executeSql(String.format("CREATE TABLE IF NOT EXISTS %s (id BIGSERIAL PRIMARY KEY, value %s)", table, jdbcType));
        debeziumHelper.addTableToPublication(publication, table);
        debeziumHelper.executeSql(String.format("INSERT INTO %s(value) VALUES (%s)", table, inputValue));
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, publication, topicPrefix, ignored -> {});
        debeziumHelper.executeSql(String.format("INSERT INTO public.signals(id, type, data) VALUES (gen_random_uuid(), 'execute-snapshot', '{\"type\": \"BLOCKING\", \"data-collections\": [\"%s\"]}')", table));

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        var schema = debeziumHelper.getLatestSchema(subject);

        assertEquals(expectedValue, msg.getFirst().getNestedRecord("after").get("value").toString());
        assertEquals(expectedAvroType, schema.after().getField("value").schema().toString());
    }

    @Disabled("This is just a list of reference types and values generated without converter during initial incremental snapshot")
    @ParameterizedTest
    @CsvSource(delimiter = ',', quoteCharacter = '*', textBlock = """
            # CASE                                                        JDBC_TYPE                                                           VALUE                       EXPECTED_VALUE                    EXPECTED_AVRO_TYPE
            reference_incremental_optional_timestamp_no_tz,               *TIMESTAMP WITHOUT TIME ZONE*,                                      *'2025-01-01 12:00:00'*,    *1735732800000000*,               *["null",{"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTimestamp"}]*
            reference_incremental_optional_timestamp_no_tz_with_default,  *TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP*,            *'2025-01-01 12:00:00'*,    *1735732800000000*,               *[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTimestamp"},"null"]*
            reference_incremental_required_timestamp_no_tz,               *TIMESTAMP WITHOUT TIME ZONE NOT NULL*,                             *'2025-01-01 12:00:00'*,    *1735732800000000*,               *{"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTimestamp"}*
            reference_incremental_required_timestamp_no_tz_with_default,  *TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,   *'2025-01-01 12:00:00'*,    *1735732800000000*,               *{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTimestamp"}*
            reference_incremental_optional_timestamp_tz,                  *TIMESTAMP WITH TIME ZONE*,                                         *'2025-01-01 12:00:00'*,    *2025-01-01T09:00:00.000000Z*,    *["null",{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTimestamp"}]*
            reference_incremental_optional_timestamp_tz_with_default,     *TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP*,               *'2025-01-01 12:00:00'*,    *2025-01-01T09:00:00.000000Z*,    *[{"type":"string","connect.version":1,"connect.default":"1970-01-01T00:00:00.000000Z","connect.name":"io.debezium.time.ZonedTimestamp"},"null"]*
            reference_incremental_required_timestamp_tz,                  *TIMESTAMP WITH TIME ZONE NOT NULL*,                                *'2025-01-01 12:00:00'*,    *2025-01-01T09:00:00.000000Z*,    *{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTimestamp"}*
            reference_incremental_required_timestamp_tz_with_default,     *TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,      *'2025-01-01 12:00:00'*,    *2025-01-01T09:00:00.000000Z*,    *{"type":"string","connect.version":1,"connect.default":"1970-01-01T00:00:00.000000Z","connect.name":"io.debezium.time.ZonedTimestamp"}*
            reference_incremental_optional_date,                          *DATE*,                                                             *'2025-01-01'*,             *20089*,                          *["null",{"type":"int","connect.version":1,"connect.name":"io.debezium.time.Date"}]*
            reference_incremental_optional_date_with_default,             *DATE DEFAULT NOW()*,                                               *'2025-01-01'*,             *20089*,                          *[{"type":"int","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Date"},"null"]*
            reference_incremental_required_date,                          *DATE NOT NULL*,                                                    *'2025-01-01'*,             *20089*,                          *{"type":"int","connect.version":1,"connect.name":"io.debezium.time.Date"}*
            reference_incremental_required_date_with_default,             *DATE NOT NULL DEFAULT NOW()*,                                      *'2025-01-01'*,             *20089*,                          *{"type":"int","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Date"}*
            reference_incremental_optional_time_no_tz,                    *TIME WITHOUT TIME ZONE*,                                           *'12:00:00'*,               *43200000000*,                    *["null",{"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTime"}]*
            reference_incremental_optional_time_no_tz_with_default,       *TIME WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP*,                 *'12:00:00'*,               *43200000000*,                    *[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTime"},"null"]*
            reference_incremental_required_time_no_tz,                    *TIME WITHOUT TIME ZONE NOT NULL*,                                  *'12:00:00'*,               *43200000000*,                    *{"type":"long","connect.version":1,"connect.name":"io.debezium.time.MicroTime"}*
            reference_incremental_required_time_no_tz_with_default,       *TIME WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,        *'12:00:00'*,               *43200000000*,                    *{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTime"}*
            reference_incremental_optional_time_tz,                       *TIME WITH TIME ZONE*,                                              *'12:00:00'*,               *09:00:00Z*,                      *["null",{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTime"}]*
            reference_incremental_optional_time_tz_with_default,          *TIME WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP*,                    *'12:00:00'*,               *09:00:00Z*,                      *["null",{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTime"}]*
            reference_incremental_required_time_tz,                       *TIME WITH TIME ZONE NOT NULL*,                                     *'12:00:00'*,               *09:00:00Z*,                      *{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTime"}*
            reference_incremental_required_time_tz_with_default,          *TIME WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,           *'12:00:00'*,               *09:00:00Z*,                      *{"type":"string","connect.version":1,"connect.name":"io.debezium.time.ZonedTime"}*
            """)
    public void referenceTypesAndValuesForIncrementalSnapshot(
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

        debeziumHelper.setupPublication(testCase);
        debeziumHelper.executeSql(String.format("CREATE TABLE IF NOT EXISTS %s (id BIGSERIAL PRIMARY KEY, value %s)", table, jdbcType));
        debeziumHelper.addTableToPublication(publication, table);
        debeziumHelper.executeSql(String.format("INSERT INTO %s(value) VALUES (%s)", table, inputValue));
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, publication, topicPrefix, ignored -> {});
        debeziumHelper.executeSql(String.format("INSERT INTO public.signals(id, type, data) VALUES (gen_random_uuid(), 'execute-snapshot', '{\"type\": \"INCREMENTAL\", \"data-collections\": [\"%s\"]}')", table));

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        var schema = debeziumHelper.getLatestSchema(subject);

        assertEquals(expectedValue, msg.getFirst().getNestedRecord("after").get("value").toString());
        assertEquals(expectedAvroType, schema.after().getField("value").schema().toString());
    }

    @ParameterizedTest
    @CsvSource(delimiter = ',', quoteCharacter = '*', textBlock = """
            # CASE                                  JDBC_TYPE                                                           VALUE                       EXPECTED_VALUE                  EXPECTED_AVRO_TYPE
            optional_timestamp_no_tz,               *TIMESTAMP WITHOUT TIME ZONE*,                                      *'2025-01-01 12:00:00'*,    *2025-01-01T12:00:00.000*,      *["null","string"]*
            optional_timestamp_no_tz_with_default,  *TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP*,            *'2025-01-01 12:00:00'*,    *2025-01-01T12:00:00.000*,      *[{"type":"string","connect.default":"1970-01-01T00:00:00.000"},"null"]*
            required_timestamp_no_tz,               *TIMESTAMP WITHOUT TIME ZONE NOT NULL*,                             *'2025-01-01 12:00:00'*,    *2025-01-01T12:00:00.000*,      *["null","string"]*
            required_timestamp_no_tz_with_default,  *TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,   *'2025-01-01 12:00:00'*,    *2025-01-01T12:00:00.000*,      *[{"type":"string","connect.default":"1970-01-01T00:00:00.000"},"null"]*
            optional_timestamp_tz,                  *TIMESTAMP WITH TIME ZONE*,                                         *'2025-01-01 12:00:00'*,    *1735722000000*,                *["null",{"type":"long","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Timestamp","logicalType":"timestamp-millis"}]*
            optional_timestamp_tz_with_default,     *TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP*,               *'2025-01-01 12:00:00'*,    *1735722000000*,                *[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"org.apache.kafka.connect.data.Timestamp","logicalType":"timestamp-millis"},"null"]*
            required_timestamp_tz,                  *TIMESTAMP WITH TIME ZONE NOT NULL*,                                *'2025-01-01 12:00:00'*,    *1735722000000*,                *["null",{"type":"long","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Timestamp","logicalType":"timestamp-millis"}]*
            required_timestamp_tz_with_default,     *TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,      *'2025-01-01 12:00:00'*,    *1735722000000*,                *[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"org.apache.kafka.connect.data.Timestamp","logicalType":"timestamp-millis"},"null"]*
            optional_date,                          *DATE*,                                                             *'2025-01-01'*,             *20089*,                        *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Date","logicalType":"date"}]*
            optional_date_with_default,             *DATE DEFAULT NOW()*,                                               *'2025-01-01'*,             *20089*,                        *[{"type":"int","connect.version":1,"connect.default":0,"connect.name":"org.apache.kafka.connect.data.Date","logicalType":"date"},"null"]*
            required_date,                          *DATE NOT NULL*,                                                    *'2025-01-01'*,             *20089*,                        *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Date","logicalType":"date"}]*
            required_date_with_default,             *DATE NOT NULL DEFAULT NOW()*,                                      *'2025-01-01'*,             *20089*,                        *[{"type":"int","connect.version":1,"connect.default":0,"connect.name":"org.apache.kafka.connect.data.Date","logicalType":"date"},"null"]*
            optional_time_no_tz,                    *TIME WITHOUT TIME ZONE*,                                           *'12:00:00'*,               *12:00:00.000*,                 *["null","string"]*
            optional_time_no_tz_with_default,       *TIME WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP*,                 *'12:00:00'*,               *12:00:00.000*,                 *[{"type":"string","connect.default":"00:00:00.000"},"null"]*
            required_time_no_tz,                    *TIME WITHOUT TIME ZONE NOT NULL*,                                  *'12:00:00'*,               *12:00:00.000*,                 *["null","string"]*
            required_time_no_tz_with_default,       *TIME WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,        *'12:00:00'*,               *12:00:00.000*,                 *[{"type":"string","connect.default":"00:00:00.000"},"null"]*
            optional_time_tz,                       *TIME WITH TIME ZONE*,                                              *'12:00:00'*,               *32400000*,                     *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Time","logicalType":"time-millis"}]*
            optional_time_tz_with_default,          *TIME WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP*,                    *'12:00:00'*,               *32400000*,                     *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Time","logicalType":"time-millis"}]*
            required_time_tz,                       *TIME WITH TIME ZONE NOT NULL*,                                     *'12:00:00'*,               *32400000*,                     *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Time","logicalType":"time-millis"}]*
            required_time_tz_with_default,          *TIME WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP*,           *'12:00:00'*,               *32400000*,                     *["null",{"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Time","logicalType":"time-millis"}]*
            """)
    public void convertTypesAndValuesUsingConverter(
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

        debeziumHelper.setupPublication(testCase);
        debeziumHelper.executeSql(String.format("CREATE TABLE IF NOT EXISTS %s (id BIGSERIAL PRIMARY KEY, value %s)", table, jdbcType));
        debeziumHelper.addTableToPublication(publication, table);
        debeziumHelper.setupPostgresDebeziumConnectorAvroFormat(CONNECTOR_NAME, publication, topicPrefix, connector -> {
            connector.with("converters", "timestampConverter");
            connector.with("timestampConverter.type", "com.nryanov.kafka.connect.toolkit.debezium.converters.TimestampConverter");
        });
        debeziumHelper.executeSql(String.format("INSERT INTO %s(value) VALUES (%s)", table, inputValue));

        var msg = debeziumHelper.readAvroMessages(topic, 1);
        var schema = debeziumHelper.getLatestSchema(subject);

        assertEquals(expectedValue, msg.getFirst().getNestedRecord("after").get("value").toString());
        assertEquals(expectedAvroType, schema.after().getField("value").schema().toString());
    }
}
