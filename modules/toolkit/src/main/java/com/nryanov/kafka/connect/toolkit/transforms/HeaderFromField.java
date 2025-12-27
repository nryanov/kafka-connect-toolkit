package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.domain.common.ConfigParser;
import com.nryanov.kafka.connect.toolkit.transforms.domain.model.FieldFilter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;

import java.util.Map;

public class HeaderFromField<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
    private final static String FIELDS = "fields";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "List of fields which should be modified. Allowed values: NULL (no fields will be modified), * (all fields), concrete list of fields"
                    );

    private FieldFilter fieldsFilter;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);

        var fieldsRaw = config.getString(FIELDS);

        if (fieldsRaw == null) {
            throw new DataException("Empty `fields` parameter");
        } else if ("*".equals(fieldsRaw)) {
            fieldsFilter = new FieldFilter.All();
        } else {
            fieldsFilter = new FieldFilter.Subset(ConfigParser.parseCommaSeparatedSingleValues(fieldsRaw));
        }
    }

    @Override
    public R apply(R record) {
        return super.apply(record);
    }
}
