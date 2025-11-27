package com.nryanov.kafka.connect.toolkit.fixtures.debezium;

import io.debezium.spi.converter.RelationalColumn;

import java.util.OptionalInt;

public class MockRelationalColumn implements RelationalColumn {
    private final String columnName;
    private final String typeName;

    public MockRelationalColumn(String columnName, String typeName) {
        this.columnName = columnName;
        this.typeName = typeName;
    }

    @Override
    public String name() {
        return columnName;
    }

    @Override
    public String dataCollection() {
        return "";
    }

    @Override
    public String charsetName() {
        return RelationalColumn.super.charsetName();
    }

    @Override
    public int jdbcType() {
        return 0;
    }

    @Override
    public int nativeType() {
        return 0;
    }

    @Override
    public String typeName() {
        return typeName;
    }

    @Override
    public String typeExpression() {
        return "";
    }

    @Override
    public OptionalInt length() {
        return OptionalInt.empty();
    }

    @Override
    public OptionalInt scale() {
        return OptionalInt.empty();
    }

    @Override
    public boolean isOptional() {
        return false;
    }

    @Override
    public Object defaultValue() {
        return null;
    }

    @Override
    public boolean hasDefaultValue() {
        return false;
    }
}
