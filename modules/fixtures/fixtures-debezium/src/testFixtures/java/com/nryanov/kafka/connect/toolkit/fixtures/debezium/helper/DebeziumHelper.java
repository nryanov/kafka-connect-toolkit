package com.nryanov.kafka.connect.toolkit.fixtures.debezium.helper;

import com.nryanov.kafka.connect.toolkit.fixtures.debezium.DebeziumFixtureContainer;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;

public class DebeziumHelper {
    private final DebeziumFixtureContainer debezium;

    public DebeziumHelper(DebeziumFixtureContainer debezium) {
        this.debezium = debezium;
    }

    public void registerConnector(String name, ConnectorConfiguration configuration) {
        debezium.getContainer().registerConnector(name, configuration);
        debezium.getContainer().ensureConnectorRegistered(name);
    }

    public void deleteConnector(String name) {
        debezium.getContainer().deleteConnector(name);
    }

    public boolean isConnectorRunning(String name) {
        return getConnectorState(name) == Connector.State.RUNNING;
    }

    private Connector.State getConnectorState(String name) {
        return debezium.getContainer().getConnectorState(name);
    }
}
