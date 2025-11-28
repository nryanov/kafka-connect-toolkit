package com.nryanov.kafka.connect.toolkit.fixtures.postgres;

import java.io.IOException;

public class PostgresHelper {
    private final PostgresFixtureContainer postgres;

    public PostgresHelper(PostgresFixtureContainer postgres) {
        this.postgres = postgres;
    }

    public void truncateTable(String table) {
        try {
            var pg = postgres.getPostgres();
            pg.execInContainer("psql", "-U", pg.getUsername(), "-d", pg.getDatabaseName(), "-c", String.format("TRUNCATE %s", table));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
