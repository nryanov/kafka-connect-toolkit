package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.domain.common.ConfigParser;
import com.nryanov.kafka.connect.toolkit.transforms.domain.hash.HashAlgorithm;
import com.nryanov.kafka.connect.toolkit.transforms.domain.hash.Hex;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class StringToHash<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String FIELDS = "fields";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Field name which should be hashed. Must be of STRING type. May be nested. Format: {field}:{algorithm}[,*]"
                    );

    private Map<String, MessageDigest> mapping;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                keySchema(record),
                key(record),
                valueSchema(record),
                value(record),
                record.timestamp()
        );
    }

    protected Schema keySchema(R record) {
        return record.keySchema();
    }

    protected Schema valueSchema(R record) {
        return record.valueSchema();
    }

    protected Object key(R record) {
        return record.key();
    }

    protected Object value(R record) {
        return record.value();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);
        var mappingsRaw = ConfigParser.parseCommaSeparatedPairs(config, FIELDS);

        mapping = new HashMap<>();

        try {
            for (var entry : mappingsRaw.entrySet()) {
                var field = entry.getKey();
                var algorithmRaw = entry.getValue();

                var algorithm = switch (HashAlgorithm.valueOf(algorithmRaw.toUpperCase())) {
                    case MD5 -> MessageDigest.getInstance("MD5");
                    case SHA1 -> MessageDigest.getInstance("SHA1");
                    case SHA256 -> MessageDigest.getInstance("SHA-256");
                    case null -> throw new DataException("Unknown hash algorithm: " + algorithmRaw);
                };

                mapping.put(field, algorithm);
            }
        } catch (NoSuchAlgorithmException e) {
            throw new DataException(e.getMessage());
        }
    }

    protected Object copyPayload(String field, Schema source, Object input) {
        if (input == null) {
            return null;
        }

        return switch (source.type()) {
            case STRUCT -> copyStruct(field, source, input);
            case ARRAY -> copyArray(field, source.valueSchema(), input);
            case STRING -> {
                if (mapping.containsKey(field)) {
                    var string = (String) input;
                    var hash = mapping.get(field);
                    yield Hex.bytesToHex(hash.digest(string.getBytes(StandardCharsets.UTF_8)));
                }

                yield input;
            }
            case null, default -> input;
        };
    }

    @SuppressWarnings("unchecked")
    protected List<Object> copyArray(String parent, Schema source, Object input) {
        var inputObjects = (List<Object>) input;

        return inputObjects.stream().map(it -> copyPayload(parent, source, it)).toList();
    }

    private Struct copyStruct(String parent, Schema source, Object input) {
        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(source);

        for (var field : source.fields()) {
            var fieldFullPath = "".equals(parent) ? field.name() : parent + "." + field.name();
            var currentValue = currentStruct.get(field);
            var currentSchema = field.schema();

            newStruct.put(field.name(), copyPayload(fieldFullPath, currentSchema, currentValue));
        }

        return newStruct;
    }

    public static class Key<R extends ConnectRecord<R>> extends StringToHash<R> {
        @Override
        protected Object key(R record) {
            var initialParentPath = "";
            return copyPayload(initialParentPath, record.keySchema(), record.key());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends StringToHash<R> {
        @Override
        protected Object value(R record) {
            var initialParentPath = "";
            return copyPayload(initialParentPath, record.valueSchema(), record.value());
        }
    }
}
