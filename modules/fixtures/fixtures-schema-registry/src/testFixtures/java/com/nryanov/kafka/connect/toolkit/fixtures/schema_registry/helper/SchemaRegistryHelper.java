package com.nryanov.kafka.connect.toolkit.fixtures.schema_registry.helper;

import com.nryanov.kafka.connect.toolkit.fixtures.schema_registry.SchemaRegistryFixtureContainer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.List;

public class SchemaRegistryHelper {
    private final SchemaRegistryClient client;

    public SchemaRegistryHelper(SchemaRegistryFixtureContainer container) {
        this.client = new CachedSchemaRegistryClient(container.confluentUrl(), 10);
    }

    public List<String> subjects() {
        try {
            return client.getAllSubjects().stream().toList();
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }
}
