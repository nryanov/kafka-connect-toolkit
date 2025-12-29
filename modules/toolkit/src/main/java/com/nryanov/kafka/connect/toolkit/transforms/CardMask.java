package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.domain.masking.CardMaskingConfig;
import com.nryanov.kafka.connect.toolkit.transforms.domain.masking.CardMaskingService;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class CardMask<R extends ConnectRecord<R>> extends AbstractBaseTransform<R> {
    private final static String FIELDS = "fields";
    private final static String EXPOSE_FIRST_COUNT = "masking.expose-first-count";
    private final static String EXPOSE_LAST_COUNT = "masking.expose-last-count";
    private final static String MASKING_CHARACTER = "masking.character";
    private final static String SEPARATORS = "masking.separators";
    private final static String CARD_NUMBER_LOWER_BOUND = "masking.card-number-lower-bound";
    private final static String CARD_NUMBER_UPPER_BOUND = "masking.card-number-upper-bound";

    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELDS,
                            ConfigDef.Type.LIST,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            "Name of fields to mask"
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

    private final Set<String> fields = new HashSet<>();
    private CardMaskingService cardMaskingService;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        var config = new AbstractConfig(CONFIG_DEF, configs);

        var fieldsRaw = config.getList(FIELDS);
        if (fieldsRaw != null) {
            fields.addAll(fieldsRaw);
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

    protected Object applyReplacements(String parent, Schema schema, Object input) {
        if (input == null) {
            return null;
        }

        return switch (schema.type()) {
            case STRUCT -> applyReplacementsToStruct(parent, schema, input);
            case ARRAY -> applyReplacementsToArray(parent, schema, input);
            case STRING -> maskStringValue(parent, input);
            case null, default -> input;
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> applyReplacementsToArray(String parent, Schema schema, Object input) {
        var array = (List<Object>) input;

        return array.stream().map(it -> applyReplacements(parent, schema, it)).toList();
    }

    private Struct applyReplacementsToStruct(String parent, Schema schema, Object input) {
        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(schema);

        for (var field : schema.fields()) {
            var nextField = "".equals(parent) ? field.name() : parent + "." + field.name();
            newStruct.put(field.name(), applyReplacements(nextField, field.schema(), currentStruct.get(field)));
        }

        return newStruct;
    }

    private Object maskStringValue(String field, Object input) {
        var stringInput = (String) input;
        if (fields.contains(field)) {
            return cardMaskingService.maskCards(stringInput);
        }

        return input;
    }

    public static class Key<R extends ConnectRecord<R>> extends CardMask<R> {
        @Override
        protected Object key(R record, Schema updatedSchema) {
            return applyReplacements("", record.keySchema(), record.key());
        }

        @Override
        protected boolean shouldProcess(R record) {
            return record.keySchema() != null;
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends CardMask<R> {
        @Override
        protected Object value(R record, Schema updatedSchema) {
            return applyReplacements("", record.valueSchema(), record.value());
        }

        @Override
        protected boolean shouldProcess(R record) {
            return record.valueSchema() != null;
        }
    }
}
