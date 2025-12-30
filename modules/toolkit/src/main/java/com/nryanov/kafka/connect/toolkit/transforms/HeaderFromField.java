package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.core.AbstractBaseTransform;
import com.nryanov.kafka.connect.toolkit.core.common.ConfigParser;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class HeaderFromField<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
    private final static String FIELDS_TO_HEADERS = "mappings";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELDS_TO_HEADERS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Comma-separated list of pairs {field}:{header}"
                    );

    private Map<String, String> fieldToHeader;

    @Override
    public final ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public final void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);
        var mappingsRaw = config.getString(FIELDS_TO_HEADERS);

        if (mappingsRaw == null) {
            throw new DataException("mappings list is null");
        }

        fieldToHeader = ConfigParser.parseCommaSeparatedPairs(config, FIELDS_TO_HEADERS);
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        if (!shouldProcess(record)) {
            return record;
        }

        var operatingSchema = operatingSchema(record);
        var operatingValue = operatingValue(record);

        var headerValues = new HashMap<String, SchemaAndValue>();

        extractHeadersFromRecord(headerValues, "", operatingSchema, operatingValue);

        return addHeaders(record, headerValues);
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    private R addHeaders(R record, Map<String, SchemaAndValue> headerValues) {
        var headers = record.headers().duplicate();

        headerValues.forEach((field, schemaAndValue) -> {
            var headerName = fieldToHeader.get(field);

            headers.add(headerName, schemaAndValue.value(), schemaAndValue.schema());
        });

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                headers
        );
    }

    private void extractHeadersFromRecord(Map<String, SchemaAndValue> headerValues, String parent, Schema source, Object input) {
        if (input == null) {
            return;
        }

        switch (source.type()) {
            case STRUCT -> extractHeadersFromStruct(headerValues, parent, source, input);
            case null, default -> {
                if (fieldToHeader.containsKey(parent)) {
                    headerValues.put(parent, new SchemaAndValue(source, input));
                }
            }
        }
    }

    private void extractHeadersFromStruct(Map<String, SchemaAndValue> headerValues, String parent, Schema source, Object input) {
        var currentStruct = requireStruct(input, "struct required");

        for (var field : source.fields()) {
            var fieldFullPath = "".equals(parent) ? field.name() : parent + "." + field.name();
            var currentValue = currentStruct.get(field);
            var currentSchema = field.schema();

            extractHeadersFromRecord(headerValues, fieldFullPath, currentSchema, currentValue);
        }
    }

    public static class Key<R extends ConnectRecord<R>> extends HeaderFromField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected boolean shouldProcess(R record) {
            return record.keySchema() != null;
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends HeaderFromField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected boolean shouldProcess(R record) {
            return record.valueSchema() != null;
        }
    }
}
