package com.nryanov.kafka.connect.toolkit.fixtures.debezium;

import io.debezium.spi.converter.CustomConverter.Converter;
import io.debezium.spi.converter.CustomConverter.ConverterRegistration;

public class MockConverterRegistry<T> implements ConverterRegistration<T> {
    public Converter converter;
    public T schema;

    @Override
    public void register(T schema, Converter converter) {
        this.converter = converter;
        this.schema = schema;
    }
}
