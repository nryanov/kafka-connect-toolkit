package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.common.Target;
import com.nryanov.kafka.connect.toolkit.masking.CardMaskingConfig;
import com.nryanov.kafka.connect.toolkit.masking.CardMaskingService;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class CardMaskFieldValue<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String KEY_FIELDS = "key.fields";
    private final static String VALUE_FIELDS = "value.fields";
    private final static String EXPOSE_FIRST_COUNT = "masking.expose-first-count";
    private final static String EXPOSE_LAST_COUNT = "masking.expose-last-count";
    private final static String MASKING_CHARACTER = "masking.character";
    private final static String SEPARATORS = "masking.separators";
    private final static String CARD_NUMBER_LOWER_BOUND = "masking.card-number-lower-bound";
    private final static String CARD_NUMBER_UPPER_BOUND = "masking.card-number-upper-bound";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            KEY_FIELDS,
                            ConfigDef.Type.LIST,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Name of fields to mask in key part"
                    )
                    .define(
                            VALUE_FIELDS,
                            ConfigDef.Type.LIST,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Name of fields to mask in value part"
                    ).define(
                            EXPOSE_FIRST_COUNT,
                            ConfigDef.Type.INT,
                            4,
                            ConfigDef.Importance.MEDIUM,
                            "Number of symbols in prefix which should be exposed in card number after masking"
                    ).define(
                            EXPOSE_LAST_COUNT,
                            ConfigDef.Type.INT,
                            4,
                            ConfigDef.Importance.MEDIUM,
                            "Number of symbols in suffix which should be exposed in card number after masking"
                    ).define(
                            MASKING_CHARACTER,
                            ConfigDef.Type.STRING,
                            "*",
                            ConfigDef.Importance.MEDIUM,
                            "Character which should be used as a masking character"
                    ).define(
                            SEPARATORS,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Comma-separated list of characters which should be considered as allowed separators in card number"
                    ).define(
                            CARD_NUMBER_LOWER_BOUND,
                            ConfigDef.Type.INT,
                            15,
                            ConfigDef.Range.atLeast(1),
                            ConfigDef.Importance.HIGH,
                            "Minimum length of card number"
                    ).define(
                            CARD_NUMBER_UPPER_BOUND,
                            ConfigDef.Type.INT,
                            16,
                            ConfigDef.Range.atLeast(1),
                            ConfigDef.Importance.HIGH,
                            "Maximum length of card number"
                    );

    private final Set<String> keyFields = new HashSet<>();
    private final Set<String> valueFields = new HashSet<>();
    private CardMaskingService cardMaskingService;

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

        var keys = config.getList(KEY_FIELDS);
        if (keys != null) {
            keyFields.addAll(keys);
        }

        var values = config.getList(VALUE_FIELDS);
        if (values != null) {
            valueFields.addAll(values);
        }

        var exposeFirst = config.getInt(EXPOSE_FIRST_COUNT);
        var exposeLast = config.getInt(EXPOSE_LAST_COUNT);
        var maskingCharacter = config.getString(MASKING_CHARACTER);
        // intentionally get raw value
        var rawSeparators = (String) configs.get(SEPARATORS);
        rawSeparators = rawSeparators == null ? "-  " : rawSeparators;
        var separators = rawSeparators.chars().mapToObj(c -> (char) c).toList();
        var cardNumberLowerBound = config.getInt(CARD_NUMBER_LOWER_BOUND);
        var cardNumberUpperBound = config.getInt(CARD_NUMBER_UPPER_BOUND);

        if (maskingCharacter == null || maskingCharacter.length() != 1) {
            throw new IllegalArgumentException("Incorrect 'masking.character' value. Expected single character but got" + maskingCharacter);
        }

        if (cardNumberLowerBound > cardNumberUpperBound) {
            throw new IllegalArgumentException(String.format(
                    "Incorrect 'masking.card-number-lower-bound' and 'masking.card-number-upper-bound' values. Expected lower <= upper but got (lower) %s & (upper) %s",
                    cardNumberLowerBound,
                    cardNumberUpperBound
            ));
        }

        if (exposeFirst + exposeLast > cardNumberLowerBound) {
            throw new IllegalArgumentException(String.format(
                    "Incorrect 'masking.expose-first-count' and 'masking.expose-last-count' values. Expected expose-first + expose-last < cardNumberLowerBound but got: %s + %s >= %s",
                    exposeFirst,
                    exposeLast,
                    cardNumberLowerBound
            ));
        }

        var maskingConfig = new CardMaskingConfig(
                maskingCharacter.charAt(0),
                separators,
                exposeFirst,
                exposeLast,
                cardNumberLowerBound,
                cardNumberUpperBound
        );

        this.cardMaskingService = new CardMaskingService(maskingConfig);
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        var initialParentPath = "";

        var mappedKey = record.key();
        if (!keyFields.isEmpty() && mappedKey != null) {
            mappedKey = applyReplacements(Target.KEY, initialParentPath, record.keySchema(), mappedKey);
        }

        var mappedValue = record.value();
        if (!valueFields.isEmpty() && mappedValue != null) {
            mappedValue = applyReplacements(Target.VALUE, initialParentPath, record.valueSchema(), mappedValue);
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                mappedKey,
                record.valueSchema(),
                mappedValue,
                record.timestamp()
        );
    }

    private Object applyReplacements(Target target, String parent, Schema schema, Object input) {
        if (schema == null) {
            return null;
        }

        return switch (schema.type()) {
            case STRUCT -> applyReplacementsToStruct(target, parent, schema, input);
            case ARRAY -> applyReplacementsToArray(target, parent, schema, input);
            case STRING -> maskStringValue(target, parent, input);
            case null, default -> input;
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> applyReplacementsToArray(Target target, String parent, Schema schema, Object input) {
        var array = (List<Object>) input;

        return array.stream().map(it -> applyReplacements(target, parent, schema, it)).toList();
    }

    private Struct applyReplacementsToStruct(Target target, String parent, Schema schema, Object input) {
        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(schema);

        for (var field : schema.fields()) {
            var nextField = "".equals(parent) ? field.name() : parent + "." + field.name();
            newStruct.put(field, applyReplacements(target, nextField, field.schema(), currentStruct.get(field)));
        }

        return newStruct;
    }

    private Object maskStringValue(Target target, String field, Object input) {
        var stringInput = (String) input;
        return switch (target) {
            case KEY -> {
                if (keyFields.contains(field)) {
                    yield cardMaskingService.maskCards(stringInput);
                }

                yield input;
            }
            case VALUE -> {
                if (valueFields.contains(field)) {
                    yield cardMaskingService.maskCards(stringInput);
                }

                yield input;
            }
        };
    }
}
