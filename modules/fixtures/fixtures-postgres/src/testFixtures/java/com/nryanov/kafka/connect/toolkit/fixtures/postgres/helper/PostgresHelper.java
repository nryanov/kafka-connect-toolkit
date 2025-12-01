package com.nryanov.kafka.connect.toolkit.fixtures.postgres.helper;

import com.nryanov.kafka.connect.toolkit.fixtures.postgres.PostgresFixtureContainer;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalConnectionPoolConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import io.agroal.pool.DataSource;
import java.io.IOException;
import java.time.Duration;
import java.util.function.Function;

public class PostgresHelper {
    private final PostgresFixtureContainer postgres;

    private final Jdbi jdbi;


    public PostgresHelper(PostgresFixtureContainer postgres) {
        this.postgres = postgres;

        var agroalConnectionFactoryConfiguration = new AgroalConnectionFactoryConfigurationSupplier();
        agroalConnectionFactoryConfiguration.jdbcUrl(postgres.jdbcUrl());
        agroalConnectionFactoryConfiguration.principal(new NamePrincipal(postgres.username()));
        agroalConnectionFactoryConfiguration.credential(new SimplePassword(postgres.password()));
        agroalConnectionFactoryConfiguration.jdbcTransactionIsolation(AgroalConnectionFactoryConfiguration.TransactionIsolation.READ_COMMITTED);

        var agroalConnectionPoolConfiguration = new AgroalConnectionPoolConfigurationSupplier();
        agroalConnectionPoolConfiguration.initialSize(1);
        agroalConnectionPoolConfiguration.minSize(1);
        agroalConnectionPoolConfiguration.maxSize(1);
        agroalConnectionPoolConfiguration.maxLifetime(Duration.ZERO);
        agroalConnectionPoolConfiguration.connectionFactoryConfiguration(agroalConnectionFactoryConfiguration);

        var agroalConfiguration = new AgroalDataSourceConfigurationSupplier();
        agroalConfiguration.connectionPoolConfiguration(agroalConnectionPoolConfiguration);
        agroalConfiguration.metricsEnabled(false);

        DataSource dataSource = new DataSource(agroalConfiguration.get());
        this.jdbi = Jdbi.create(dataSource);
    }

    public void truncateTable(String table) {
        try {
            var pg = postgres.getPostgres();
            pg.execInContainer("psql", "-U", pg.getUsername(), "-d", pg.getDatabaseName(), "-c", String.format("TRUNCATE %s", table));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public <R> R inTransaction(Function<Handle, R> code) {
        try {
            return jdbi.<R, Exception>inTransaction(code::apply);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
