package com.nryanov.kafka.connect.toolkit.fixtures.kafka_connect.helper;

import com.nryanov.kafka.connect.toolkit.fixtures.kafka_connect.KafkaConnectFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaAdminHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaRawConsumerHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaRawProducerHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.postgres.helper.PostgresHelper;

public class KafkaConnectHelper {
    private final KafkaAdminHelper adminHelper;
    private final KafkaRawConsumerHelper consumerHelper;
    private final KafkaRawProducerHelper producerHelper;

    private final PostgresHelper postgresHelper;

    public KafkaConnectHelper(KafkaConnectFixtureContainer kafkaConnect) {
        this.adminHelper = new KafkaAdminHelper(kafkaConnect.getKafka());
        this.consumerHelper = new KafkaRawConsumerHelper(kafkaConnect.getKafka());
        this.producerHelper = new KafkaRawProducerHelper(kafkaConnect.getKafka());

        this.postgresHelper = new PostgresHelper(kafkaConnect.getPostgres());
    }
}
