package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.ConfigParser;
import com.nryanov.kafka.connect.toolkit.transforms.common.SchemaCopyUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.data.Schema.Type.STRUCT;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ConcatFields<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String INPUT_FIELDS = "input.fields";
    private final static String INPUT_FIELDS_NULL_REPLACEMENT = "input.fields.null-replacement";
    private final static String OUTPUT_FIELD = "output.field";
    private final static String DEFAULT_DELIMITER = "delimiter";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            INPUT_FIELDS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Comma separated list of fields for concatenation. Fields will be concatenated in the specified order"
                    )
                    .define(
                            INPUT_FIELDS_NULL_REPLACEMENT,
                            ConfigDef.Type.STRING,
                            "",
                            ConfigDef.Importance.MEDIUM,
                            "Default value which should be used if field's value is null"
                    )
                    .define(
                            OUTPUT_FIELD,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Output field name"
                    )
                    .define(
                            DEFAULT_DELIMITER,
                            ConfigDef.Type.STRING,
                            "_",
                            ConfigDef.Importance.LOW,
                            "Default delimiter"
                    );

    private LinkedHashSet<String> filter;
    private String outputField;
    private String nullReplacement;
    private String delimiter;

    @Override
    public final ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public final void close() {

    }

    @Override
    public final void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);
        var fieldsRaw = config.getString(INPUT_FIELDS);

        filter = ConfigParser.parseCommaSeparatedSingleValuesPreserveOrder(fieldsRaw);
        outputField = config.getString(OUTPUT_FIELD);
        nullReplacement = config.getString(INPUT_FIELDS_NULL_REPLACEMENT);
        delimiter = config.getString(DEFAULT_DELIMITER);
    }

    protected void setConcatenationValue(Map<String, List<Object>> concat, Object target) {
        var struct = requireStruct(target, "struct");
        var result = new StringBuilder();

        var firstField = filter.getFirst();
        var firstRawValue = concat.get(firstField);
        var firstValue = firstRawValue == null ? nullReplacement : firstRawValue
                .stream()
                .map(it -> it == null ? nullReplacement : it.toString())
                .collect(Collectors.joining(delimiter));

        result.append(firstValue);

        filter.stream().skip(1).forEach(field -> {
            result.append(delimiter);
            var rawValue = concat.get(field);
            var value = rawValue == null ? nullReplacement : rawValue
                    .stream()
                    .map(it -> it == null ? nullReplacement : it.toString())
                    .collect(Collectors.joining(delimiter));

            result.append(value);
        });

        struct.put(outputField, result.toString());
    }

    protected Schema addFieldToSchema(Schema source) {
        if (!STRUCT.equals(source.type())) {
            throw new DataException("Expected struct but got: " + source.type());
        }

        var copiedSchema = SchemaCopyUtil.copySchemaBasics(source);

        for (var field : source.fields()) {
            copiedSchema.field(field.name(), field.schema());
        }

        // explicitly set defaultValue for optional field
        copiedSchema.field(outputField, SchemaBuilder.string().optional().defaultValue(null).build());

        return copiedSchema.build();
    }

    protected Object copyValuesToNewSchema(Map<String, List<Object>> concat, String parent, Schema source, Schema target, Object input) {
        return switch (source.type()) {
            // explicitly handle array to correctly concat all items in the selected array field(s)
            case ARRAY -> copyArray(concat, parent, source.valueSchema(), target.valueSchema(), input);
            case STRUCT -> copyStruct(concat, parent, source, target, input);
            case null, default -> {
                if (filter.contains(parent)) {
                    var current = concat.get(parent);
                    if (current == null) {
                        current = new ArrayList<>();
                    }
                    current.add(input);
                    concat.put(parent, current);
                }

                yield input;
            }
        };
    }

    @SuppressWarnings("unchecked")
    protected List<Object> copyArray(Map<String, List<Object>> concat, String parent, Schema source, Schema target, Object input) {
        var inputObjects = (List<Object>) input;

        return inputObjects.stream().map(it -> copyValuesToNewSchema(concat, parent, source, target, it)).toList();
    }

    protected Struct copyStruct(Map<String, List<Object>> concat, String parent, Schema source, Schema target, Object input) {
        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(target);

        for (var field : source.fields()) {
            var fieldFullPath = "".equals(parent) ? field.name() : parent + "." + field.name();
            var currentValue = currentStruct.get(field);
            var currentSchema = field.schema();
            var targetSchema = target.field(field.name()).schema();

            newStruct.put(field.name(), copyValuesToNewSchema(concat, fieldFullPath, currentSchema, targetSchema, currentValue));
        }

        return newStruct;
    }

    public static class Key<R extends ConnectRecord<R>> extends ConcatFields<R> {
        @Override
        public R apply(R record) {
            if (record == null) {
                return null;
            }

            var initialParentPath = "";
            var concat = new HashMap<String, List<Object>>();

            var newKeySchema = addFieldToSchema(record.keySchema());

            var newKeyStruct = record.key();
            if (newKeyStruct != null) {
                newKeyStruct = copyValuesToNewSchema(concat, initialParentPath, record.keySchema(), newKeySchema, record.key());
                setConcatenationValue(concat, newKeyStruct);
            }

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    newKeySchema,
                    newKeyStruct,
                    record.valueSchema(),
                    record.value(),
                    record.timestamp()
            );
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ConcatFields<R> {
        public R apply(R record) {
            if (record == null) {
                return null;
            }

            var initialParentPath = "";
            var concat = new HashMap<String, List<Object>>();

            var newValueSchema = addFieldToSchema(record.valueSchema());

            var newValueStruct = record.value();
            if (newValueStruct != null) {
                newValueStruct = copyValuesToNewSchema(concat, initialParentPath, record.valueSchema(), newValueSchema, record.value());
                setConcatenationValue(concat, newValueStruct);
            }

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    newValueSchema,
                    newValueStruct,
                    record.timestamp()
            );
        }
    }
}
