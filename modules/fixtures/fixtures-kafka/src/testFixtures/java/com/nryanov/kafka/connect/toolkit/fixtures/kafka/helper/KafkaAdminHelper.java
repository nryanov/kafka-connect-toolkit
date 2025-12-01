package com.nryanov.kafka.connect.toolkit.fixtures.kafka.helper;

import com.nryanov.kafka.connect.toolkit.fixtures.kafka.KafkaFixtureContainer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaAdminHelper {
    private final static long DEFAULT_WAIT_TIMEOUT = 5;
    private final AdminClient adminClient;

    public KafkaAdminHelper(KafkaFixtureContainer container) {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, container.bootstrapServers());

        this.adminClient = KafkaAdminClient.create(properties);
    }

    public void createTopic(String topic) {
        try {
            var newTopic = new NewTopic(topic, 1, (short) 1);
            adminClient.createTopics(List.of(newTopic)).all().get(DEFAULT_WAIT_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteTopic(String topic) {
        try {
            adminClient.deleteTopics(List.of(topic)).all().get(DEFAULT_WAIT_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> topics() {
        try {
            return adminClient
                    .listTopics()
                    .names()
                    .get(DEFAULT_WAIT_TIMEOUT, TimeUnit.SECONDS)
                    .stream()
                    .toList();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
