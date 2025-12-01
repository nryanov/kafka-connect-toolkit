package com.nryanov.kafka.connect.toolkit.fixtures.postgres;

import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.util.Set;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class PostgresFixtureContainer implements Startable {
    private final static String NETWORK_ALIAS = "postgres";
    private final PostgreSQLContainer<?> postgres;

    public PostgresFixtureContainer() {
        postgres = new PostgreSQLContainer<>(
                DockerImageName.
                        parse("postgres:17")
                        .asCompatibleSubstituteFor("postgres")
        ).withCommand("postgres",
                        "-c", "wal_level=logical",
                        "-c", "max_wal_senders=5",
                        "-c", "max_replication_slots=5"
                )
                .withPassword("postgres")
                .withUsername("postgres")
                .withDatabaseName("postgres")
                .withNetworkAliases(NETWORK_ALIAS);
    }

    public PostgresFixtureContainer(Network network) {
        this();
        postgres.withNetwork(network);
    }

    public PostgreSQLContainer<?> getPostgres() {
        if (!postgres.isRunning()) {
            throw new RuntimeException("Postgres is not yet started");
        }
        return postgres;
    }

    @Override
    public void start() {
        postgres.start();
    }

    @Override
    public void stop() {
        postgres.stop();
    }

    @Override
    public Set<Startable> getDependencies() {
        return postgres.getDependencies();
    }

    public String jdbcUrl() {
        return postgres.getJdbcUrl();
    }

    public String networkJdbcUrl() {
        return String.format("jdbc:postgresql://%s:%s/%s", networkHost(), networkPort(), postgres.getDatabaseName());
    }

    public String host() {
        return postgres.getHost();
    }

    public String networkHost() {
        return NETWORK_ALIAS;
    }

    public int networkPort() {
        return POSTGRESQL_PORT;
    }

    public int port() {
        return postgres.getMappedPort(POSTGRESQL_PORT);
    }

    public String username() {
        return postgres.getUsername();
    }

    public String password() {
        return postgres.getPassword();
    }

    public String database() {
        return postgres.getDatabaseName();
    }
}
