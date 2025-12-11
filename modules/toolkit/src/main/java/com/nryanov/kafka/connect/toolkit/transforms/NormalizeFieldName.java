package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class NormalizeFieldName<R extends ConnectRecord<R>> implements Transformation<R> {
    enum CaseType {
        LOWER(String::toLowerCase),
        UPPER(String::toUpperCase),
        NONE(it -> it);

        private final Function<String, String> mapper;

        CaseType(Function<String, String> mapper) {
            this.mapper = mapper;
        }

        public String apply(String value) {
            return mapper.apply(value);
        }
    }

    private final static String INITIAL_CASE = "case.initial";
    private final static String TARGET_CASE = "case.target";
    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            INITIAL_CASE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Initial case of field names which should be considered for change"
                    )
                    .define(
                            TARGET_CASE,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Target case of field names"
                    );

    private CaseType initialCase;
    private CaseType targetCase;

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
        initialCase = CaseType.valueOf(Objects.requireNonNull(config.getString(INITIAL_CASE), "Empty case.initial config"));
        targetCase = CaseType.valueOf(Objects.requireNonNull(config.getString(TARGET_CASE), "Empty case.target config"));
    }

    @Override
    public R apply(R record) {
        return null;
    }

    public Schema applyMappingToSchema(Schema source) {
        var newSchema = SchemaUtil.copySchemaBasics(source);



        return newSchema.build();
    }
}
