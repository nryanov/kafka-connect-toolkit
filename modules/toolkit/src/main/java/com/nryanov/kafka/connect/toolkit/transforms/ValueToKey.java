package com.nryanov.kafka.connect.toolkit.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;

public class ValueToKey<R extends ConnectRecord<R>> extends AbstractCopyFromTo<R> {
    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        var initialParentPath = "";

        var schemaPatch = extractSchemaPatch(initialParentPath, record.valueSchema());
        var structPatch = copyValuesToNewSchema(initialParentPath, record.valueSchema(), schemaPatch, record.value());
        var mergedKeySchema = mergeSchemas(record.keySchema(), schemaPatch);
        var mergedKeyStruct = mergeStructs(mergedKeySchema, record.key(), structPatch);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                mergedKeySchema,
                mergedKeyStruct,
                record.valueSchema(),
                record.value(),
                record.timestamp()
        );
    }
}
