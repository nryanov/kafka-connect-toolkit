package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.SchemaCopyUtil;
import com.nryanov.kafka.connect.toolkit.transforms.hash.HashAlgorithm;
import com.nryanov.kafka.connect.toolkit.transforms.hash.Hex;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.connect.data.Schema.Type.STRUCT;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class StringToHash<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String INPUT_FIELDS = "fields";
    private final static String HASH_ALGORITHM = "algorithm";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            INPUT_FIELDS,
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
                    );

    private String inputField;
    private String outputField;
    private MessageDigest algorithm;

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public R apply(R record) {
        return null;
    }
}
