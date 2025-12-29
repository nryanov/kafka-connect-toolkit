package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.domain.common.SchemaCopyUtil;
import com.nryanov.kafka.connect.toolkit.transforms.domain.hash.HashAlgorithm;
import com.nryanov.kafka.connect.toolkit.transforms.domain.hash.Hex;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.connect.data.Schema.Type.STRUCT;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class InsertHash<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
    private final static String INPUT_FIELD = "input.field";
    private final static String HASH_ALGORITHM = "algorithm";
    private final static String OUTPUT_FIELD = "output.field";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            INPUT_FIELD,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Field name which should be hashed. Must be of STRING type. May be nested"
                    )
                    .define(
                            HASH_ALGORITHM,
                            ConfigDef.Type.STRING,
                            "md5",
                            ConfigDef.Importance.HIGH,
                            "Hash algorithm. By default: md5"
                    )
                    .define(
                            OUTPUT_FIELD,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Output field name. This field will be additionally added to schema. It will NOT overwrite existing one"
                    );

    private String inputField;
    private String outputField;
    private MessageDigest algorithm;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);
        inputField = config.getString(INPUT_FIELD);
        outputField = config.getString(OUTPUT_FIELD);

        if (inputField.equals(outputField)) {
            throw new DataException("INPUT field should not be equal to the OUTPUT field");
        }

        var algorithmRaw = config.getString(HASH_ALGORITHM);

        try {
            algorithm = switch (HashAlgorithm.valueOf(algorithmRaw.toUpperCase())) {
                case MD5 -> MessageDigest.getInstance("MD5");
                case SHA1 -> MessageDigest.getInstance("SHA1");
                case SHA256 -> MessageDigest.getInstance("SHA-256");
                case null -> throw new DataException("Unknown hash algorithm: " + algorithmRaw);
            };
        } catch (NoSuchAlgorithmException e) {
            throw new DataException(e.getMessage());
        }
    }

    protected void setHashValue(AtomicReference<String> hash, Object target) {
        var struct = requireStruct(target, "struct");
        var input = hash.get();

        if (input == null) {
            struct.put(outputField, null);
        } else {
            struct.put(outputField, Hex.bytesToHex(algorithm.digest(input.getBytes(StandardCharsets.UTF_8))));
        }
    }

    protected Schema addFieldToSchema(Schema source) {
        var copiedSchema = SchemaCopyUtil.copySchemaBasics(source);

        for (var field : source.fields()) {
            copiedSchema.field(field.name(), field.schema());
        }

        // explicitly set defaultValue for optional field
        copiedSchema.field(outputField, SchemaBuilder.string().optional().defaultValue(null).build());

        return copiedSchema.build();
    }

    protected Object copyValuesToNewSchema(AtomicReference<String> hash, String parent, Schema source, Schema target, Object input) {
        if (input == null) {
            return null;
        }

        return switch (source.type()) {
            case STRUCT -> copyStruct(hash, parent, source, target, input);
            case STRING -> {
                if (inputField.equals(parent)) {
                    hash.set((String) input);
                }

                yield input;
            }
            case null, default -> input;
        };
    }

    protected Struct copyStruct(AtomicReference<String> hash, String parent, Schema source, Schema target, Object input) {
        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(target);

        for (var field : source.fields()) {
            var fieldFullPath = "".equals(parent) ? field.name() : parent + "." + field.name();
            var currentValue = currentStruct.get(field);
            var currentSchema = field.schema();
            var targetSchema = target.field(field.name()).schema();

            newStruct.put(field.name(), copyValuesToNewSchema(hash, fieldFullPath, currentSchema, targetSchema, currentValue));
        }

        return newStruct;
    }

    public static class Key<R extends ConnectRecord<R>> extends InsertHash<R> {
        @Override
        protected Object key(R record, Schema updatedSchema) {
            var hash = new AtomicReference<String>();
            var newKeyStruct = record.key();

            if (newKeyStruct != null) {
                newKeyStruct = copyValuesToNewSchema(hash, "", record.keySchema(), updatedSchema, record.key());
                setHashValue(hash, newKeyStruct);
            }

            return newKeyStruct;
        }

        @Override
        protected Schema keySchema(R record) {
            return addFieldToSchema(record.keySchema());
        }

        @Override
        protected boolean shouldProcess(R record) {
            return record.keySchema() != null && STRUCT.equals(record.keySchema().type());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends InsertHash<R> {
        @Override
        protected Schema valueSchema(R record) {
            return addFieldToSchema(record.valueSchema());
        }

        @Override
        protected Object value(R record, Schema updatedSchema) {
            var hash = new AtomicReference<String>();

            var newValueStruct = record.value();
            if (newValueStruct != null) {
                newValueStruct = copyValuesToNewSchema(hash, "", record.valueSchema(), updatedSchema, record.value());
                setHashValue(hash, newValueStruct);
            }

            return newValueStruct;
        }

        @Override
        protected boolean shouldProcess(R record) {
            return record.valueSchema() != null && STRUCT.equals(record.valueSchema().type());
        }
    }
}
