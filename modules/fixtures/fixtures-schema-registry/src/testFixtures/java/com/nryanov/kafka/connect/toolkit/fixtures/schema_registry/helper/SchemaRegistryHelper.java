package com.nryanov.kafka.connect.toolkit.fixtures.schema_registry.helper;

import com.nryanov.kafka.connect.toolkit.fixtures.schema_registry.SchemaRegistryFixtureContainer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

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

    public Schema getLatestSchema(String subject) {
        try {
            var parsedSchema = client.getLatestSchemaMetadata(subject);
            return new Schema.Parser().parse(parsedSchema.getSchema());
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Integer> getSubjectVersions(String subject) {
        try {
            return client.getAllVersions(subject);
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    public Schema getSubjectSchemaByVersions(String subject, int version) {
        var parsedSchema = client.getByVersion(subject, version, false);
        return new Schema.Parser().parse(parsedSchema.getSchema());
    }
}
