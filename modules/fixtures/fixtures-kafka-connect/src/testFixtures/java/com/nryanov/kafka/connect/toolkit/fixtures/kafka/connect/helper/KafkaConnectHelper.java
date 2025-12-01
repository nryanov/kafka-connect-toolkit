package com.nryanov.kafka.connect.toolkit.fixtures.kafka.connect.helper;

import com.nryanov.kafka.connect.toolkit.fixtures.kafka.connect.KafkaConnectFixtureContainer;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaAdminHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaConsumerHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper.KafkaProducerHelper;
import com.nryanov.kafka.connect.toolkit.fixtures.postgres.helper.PostgresHelper;

public class KafkaConnectHelper {
    private final KafkaAdminHelper adminHelper;
    private final KafkaConsumerHelper consumerHelper;
    private final KafkaProducerHelper producerHelper;

    private final PostgresHelper postgresHelper;

    public KafkaConnectHelper(KafkaConnectFixtureContainer kafkaConnect) {
        this.adminHelper = new KafkaAdminHelper(kafkaConnect.getKafka());
        this.consumerHelper = new KafkaConsumerHelper(kafkaConnect.getKafka());
        this.producerHelper = new KafkaProducerHelper(kafkaConnect.getKafka());

        this.postgresHelper = new PostgresHelper(kafkaConnect.getPostgres());
    }
}
