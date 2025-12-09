package com.nryanov.kafka.connect.toolkit.debezium.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class SchemaRename<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String PURPOSE = "Access values to modify namespace";
    private final static String BEFORE_FIELD_NAME = "before";
    private final static String AFTER_FIELD_NAME = "after";
    private final static Set<String> BEFORE_AND_AFTER_FIELDS = Set.of(BEFORE_FIELD_NAME, AFTER_FIELD_NAME);

    private final static Integer DEFAULT_CACHE_SIZE = 16;
    private final static String CACHE_SIZE_CONFIG = "cache.size";
    private final static String NEW_EXTERNAL_NAME_CONFIG = "external.name";
    private final static String NEW_INTERNAL_NAME_CONFIG = "internal.name";
    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            NEW_INTERNAL_NAME_CONFIG,
                            ConfigDef.Type.STRING,
                            ConfigDef.NO_DEFAULT_VALUE,
                            ConfigDef.Importance.HIGH,
                            "The new name for the internal records in the before and after schemata. If not set, then names remain without change")
                    .define(
                            NEW_EXTERNAL_NAME_CONFIG,
                            ConfigDef.Type.STRING,
                            ConfigDef.NO_DEFAULT_VALUE,
                            ConfigDef.Importance.HIGH,
                            "The new name for the external records. If not set, then name remains without change")
                    .define(
                            CACHE_SIZE_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_CACHE_SIZE,
                            ConfigDef.Range.atLeast(1),
                            ConfigDef.Importance.LOW,
                            "Schema cache size to avoid schema updates for each new schema");

    private String newExternalName;
    private String newInternalName;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);

        newExternalName = config.getString(NEW_EXTERNAL_NAME_CONFIG);
        newInternalName = config.getString(NEW_INTERNAL_NAME_CONFIG);
        var cacheSize = config.getInt(CACHE_SIZE_CONFIG);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(cacheSize));
    }

    @Override
    public R apply(R record) {
        if (record == null || record.valueSchema() == null) return record;

        var beforeField = record.valueSchema().field(BEFORE_FIELD_NAME);
        var afterField = record.valueSchema().field(AFTER_FIELD_NAME);

        if (beforeField == null || afterField == null) return record;

        var beforeSchema = beforeField.schema();
        var afterSchema = afterField.schema();

        if (beforeSchema == null || afterSchema == null) return record;

        var updatedBeforeSchema = updateSchema(beforeSchema);
        var updatedAfterSchema = updateSchema(afterSchema);

        var recordValue = requireStruct(record.value(), PURPOSE);

        var beforeValue = recordValue.getStruct(BEFORE_FIELD_NAME);
        var afterValue = recordValue.getStruct(AFTER_FIELD_NAME);

        var updatedBeforeValue = copyValues(beforeValue, updatedBeforeSchema);
        var updatedAfterValue = copyValues(afterValue, updatedAfterSchema);

        var updatedRecordSchema = replaceBeforeAndAfterSchemata(record.valueSchema(), updatedBeforeSchema, updatedAfterSchema);
        var updatedRecordValue = replaceBeforeAndAfterValues(record, updatedRecordSchema, updatedBeforeValue, updatedAfterValue);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedRecordSchema,
                updatedRecordValue,
                record.timestamp());
    }

    private Schema updateSchema(Schema recordSchema) {
        var updatedSchema = schemaUpdateCache.get(recordSchema);

        if (updatedSchema == null) {
            updatedSchema = makeUpdatedInternalSchema(recordSchema);
            schemaUpdateCache.put(recordSchema, updatedSchema);
        }

        return updatedSchema;
    }

    private Schema makeUpdatedInternalSchema(Schema sourceSchema) {
        final var builder = SchemaBuilder.struct();

        builder.version(sourceSchema.version());
        builder.doc(sourceSchema.doc());
        builder.name(newInternalName);
        builder.optional();

        var params = sourceSchema.parameters();

        if (params != null) {
            builder.parameters(params);
        }

        for (var field : sourceSchema.fields()) {
            builder.field(field.name(), field.schema());
        }

        return builder.build();
    }

    private Struct copyValues(Struct recordValue, Schema updatedSchema) {
        if (recordValue == null) return null;
        var updatedRecordValue = new Struct(updatedSchema);

        for (var field : updatedRecordValue.schema().fields()) {
            updatedRecordValue.put(field.name(), recordValue.get(field));
        }

        return updatedRecordValue;
    }

    private Schema replaceBeforeAndAfterSchemata(Schema schema, Schema beforeSchemaReplacement, Schema afterSchemaReplacement) {
        var builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (var field : schema.fields()) {
            if (!BEFORE_AND_AFTER_FIELDS.contains(field.name())) {
                builder.field(field.name(), field.schema());
            }
        }

        builder.field(BEFORE_FIELD_NAME, beforeSchemaReplacement);
        builder.field(AFTER_FIELD_NAME, afterSchemaReplacement);

        return builder.build();
    }

    private Struct replaceBeforeAndAfterValues(R record, Schema updatedSchema, Struct beforeValueReplacement, Struct afterValueReplacement) {
        var updatedRecordValue = new Struct(updatedSchema);

        for (var field : record.valueSchema().fields()) {
            if (!BEFORE_AND_AFTER_FIELDS.contains(field.name())) {
                var originalValue = ((Struct) record.value()).get(field);
                updatedRecordValue.put(field.name(), originalValue);
            }
        }

        updatedRecordValue.put(BEFORE_FIELD_NAME, beforeValueReplacement);
        updatedRecordValue.put(AFTER_FIELD_NAME, afterValueReplacement);

        return updatedRecordValue;
    }
}
