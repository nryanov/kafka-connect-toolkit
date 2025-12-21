package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.ConfigParser;
import com.nryanov.kafka.connect.toolkit.transforms.common.FieldFiler;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;

public class ValueToKey<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String FIELDS = "fields";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Comma separated list of fields in value which should be copied to key. Fields may be nested, but in result they will be in top of structure of key"
                    );

    private Map<FieldFiler.Subset, String> mappings;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);
        var fieldMappings = ConfigParser.parseCommaSeparatedPairs(config, FIELDS);

        mappings = new HashMap<>();

        fieldMappings.forEach((source, target) -> {
            mappings.put(new FieldFiler.Subset())
        });
    }

    @Override
    public R apply(R record) {
        return null;
    }
}
