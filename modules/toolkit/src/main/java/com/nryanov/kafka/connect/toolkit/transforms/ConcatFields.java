package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.ConfigParser;
import com.nryanov.kafka.connect.toolkit.transforms.common.FieldFilter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class ConcatFields<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String FIELDS = "fields";
    private final static String DEFAULT_DELIMITER = "delimiter";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Comma separated list of fields for concatenation"
                    )
                    .define(
                            DEFAULT_DELIMITER,
                            ConfigDef.Type.STRING,
                            "_",
                            ConfigDef.Importance.MEDIUM,
                            "Default delimiter"
                    );

    private FieldFilter filter;
    private String delimiter;

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
        var fieldsRaw = config.getString(FIELDS);

        if (fieldsRaw != null && !"*".equals(fieldsRaw)) {
            filter = new FieldFilter.Subset(ConfigParser.parseCommaSeparatedSingleValues(fieldsRaw));
        } else if ("*".equals(fieldsRaw)) {
            filter = new FieldFilter.All();
        } else {
            filter = new FieldFilter.None();
        }
        delimiter = config.getString(DEFAULT_DELIMITER);
    }

    @Override
    public R apply(R record) {
        return null;
    }
}
