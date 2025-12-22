package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;

public class KeyToValue<R extends ConnectRecord<R>> extends AbstractCopyFromTo<R> {
    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        var initialParentPath = "";

        var schemaPatch = extractSchemaPatch(initialParentPath, record.keySchema());
        var structPatch = copyValuesToNewSchema(initialParentPath, record.keySchema(), schemaPatch, record.key());
        var mergedValueSchema = mergeSchemas(record.valueSchema(), schemaPatch);
        var mergedValueStruct = mergeStructs(mergedValueSchema, record.value(), structPatch);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                mergedValueSchema,
                mergedValueStruct,
                record.timestamp()
        );
    }
}
