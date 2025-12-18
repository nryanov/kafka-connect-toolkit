package com.nryanov.kafka.connect.toolkit.transforms;

import com.nryanov.kafka.connect.toolkit.transforms.common.ConfigParser;
import com.nryanov.kafka.connect.toolkit.transforms.common.FieldFiler;
import com.nryanov.kafka.connect.toolkit.transforms.common.SchemaCopyUtil;
import com.nryanov.kafka.connect.toolkit.transforms.common.Target;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class DecimalAdjustScaleAndPrecision<R extends ConnectRecord<R>> implements Transformation<R> {
    enum PrecisionMode {
        NONE, // do not change precision
        IF_NOT_SET, // change precision only if current precision is undefined
        VALUE, // unconditionally set precision
        LIMIT // limit maximum precision by provided value
    }

    enum ScaleMode {
        NONE, // do not change scale
        IF_NOT_SET, // change scale only if current scale is undefined
        VALUE, // unconditionally set scale
        LIMIT // limit maximum scale by provided value
    }

    enum ScaleZeroMode {
        NONE, // do not change scale if current scale is zero
        VALUE // set provided value as a new scale if current scale is zero
    }

    enum ScaleNegativeMode {
        NONE, // do not change scale
        VALUE // set provided value as a new scale if current scale is < 0
    }

    private record PrecisionAndScale(int precision, int scale) {
    }

    private final static String DECIMAL_PRECISION_PROPERTY = "connect.decimal.precision";
    private final static String DECIMAL_SCALE_PROPERTY = "scale";

    // NULL, * , list of concrete fields
    private final static String KEY_FIELDS = "key.fields";
    private final static String VALUE_FIELDS = "value.fields";

    private final static String PRECISION = "precision.value";
    private final static String PRECISION_MODE = "precision.mode";
    private final static String UNDEFINED_PRECISION_VALUE = "precision.undefined-value";
    private final static String SCALE = "scale.value";
    private final static String SCALE_MODE = "scale.mode";
    private final static String SCALE_ZERO_MODE = "scale.zero-mode";
    private final static String SCALE_NEGATIVE_MODE = "scale.negative-mode";
    private final static String UNDEFINED_SCALE_VALUE = "scale.undefined-value";
    private final static ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            KEY_FIELDS,
                            ConfigDef.Type.STRING,
                            "*",
                            ConfigDef.Importance.MEDIUM,
                            "List of fields in key-part which should be modified. Allowed values: NULL (no fields will be modified), * (all fields), concrete list of fields"
                    )
                    .define(
                            VALUE_FIELDS,
                            ConfigDef.Type.STRING,
                            "*",
                            ConfigDef.Importance.MEDIUM,
                            "List of fields in key-part which should be modified. Allowed values: NULL (no fields will be modified), * (all fields), concrete list of fields"
                    )
                    .define(
                            PRECISION,
                            ConfigDef.Type.INT,
                            ConfigDef.NO_DEFAULT_VALUE,
                            ConfigDef.Range.atLeast(1),
                            ConfigDef.Importance.HIGH,
                            "New precision value"
                    )
                    .define(
                            SCALE,
                            ConfigDef.Type.INT,
                            ConfigDef.NO_DEFAULT_VALUE,
                            ConfigDef.Importance.HIGH,
                            "New scale value"
                    )
                    .define(
                            UNDEFINED_PRECISION_VALUE,
                            ConfigDef.Type.INT,
                            -1,
                            ConfigDef.Importance.MEDIUM,
                            "Value which should be used as undefined for precision. Different source systems may use it's own value for it"
                    ).define(
                            UNDEFINED_SCALE_VALUE,
                            ConfigDef.Type.INT,
                            -1,
                            ConfigDef.Importance.MEDIUM,
                            "Value which should be used as undefined for scale. Different source systems may use it's own value for it"
                    ).define(
                            PRECISION_MODE,
                            ConfigDef.Type.STRING,
                            PrecisionMode.NONE.name(),
                            ConfigDef.Importance.MEDIUM,
                            "Behavior for precision value. By default -- do not change it"
                    ).define(
                            SCALE_MODE,
                            ConfigDef.Type.STRING,
                            ScaleMode.NONE.name(),
                            ConfigDef.Importance.MEDIUM,
                            "Behavior for scale value. By default -- do not change it"
                    ).define(
                            SCALE_ZERO_MODE,
                            ConfigDef.Type.STRING,
                            ScaleZeroMode.NONE.name(),
                            ConfigDef.Importance.MEDIUM,
                            "Behavior for scale value if current scale is zero. By default -- do not change it"
                    ).define(
                            SCALE_NEGATIVE_MODE,
                            ConfigDef.Type.STRING,
                            ScaleNegativeMode.NONE.name(),
                            ConfigDef.Importance.MEDIUM,
                            "Behavior for scale value if current scale is negative. By default -- do not change it"
                    );

    private FieldFiler keyFieldFilter;
    private FieldFiler valueFieldFilter;
    private int precision;
    private int scale;
    private PrecisionMode precisionMode;
    private ScaleMode scaleMode;
    private ScaleZeroMode scaleZeroMode;
    private ScaleNegativeMode scaleNegativeMode;
    private int undefinedPrecision;
    private int undefinedScale;

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

        var keyFieldsRaw = config.getString(KEY_FIELDS);
        var valueFieldsRaw = config.getString(VALUE_FIELDS);

        if (keyFieldsRaw == null) {
            keyFieldFilter = new FieldFiler.None();
        } else if ("*".equals(keyFieldsRaw)) {
            keyFieldFilter = new FieldFiler.All();
        } else {
            keyFieldFilter = new FieldFiler.Subset(ConfigParser.parseCommaSeparatedSingleValues(keyFieldsRaw));
        }

        if (valueFieldsRaw == null) {
            valueFieldFilter = new FieldFiler.None();
        } else if ("*".equals(valueFieldsRaw)) {
            valueFieldFilter = new FieldFiler.All();
        } else {
            valueFieldFilter = new FieldFiler.Subset(ConfigParser.parseCommaSeparatedSingleValues(valueFieldsRaw));
        }

        precision = config.getInt(PRECISION);
        precisionMode = PrecisionMode.valueOf(config.getString(PRECISION_MODE));
        undefinedPrecision = config.getInt(UNDEFINED_PRECISION_VALUE);

        scale = config.getInt(SCALE);
        undefinedScale = config.getInt(UNDEFINED_SCALE_VALUE);
        scaleMode = ScaleMode.valueOf(config.getString(SCALE_MODE));
        scaleZeroMode = ScaleZeroMode.valueOf(config.getString(SCALE_ZERO_MODE));
        scaleNegativeMode = ScaleNegativeMode.valueOf(config.getString(SCALE_NEGATIVE_MODE));
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        var initialParentPath = "";

        var mappedKeySchema = record.keySchema();
        var mappedKey = record.key();

        if (keyFieldFilter.enabled()) {
            mappedKeySchema = applyMappingToSchema(Target.KEY, initialParentPath, record.keySchema());
            mappedKey = copyValuesToNewSchema(Target.KEY, initialParentPath, record.keySchema(), mappedKeySchema, record.key());
        }

        var mappedValue = record.value();
        var mappedValueSchema = record.valueSchema();

        if (valueFieldFilter.enabled()) {
            mappedValueSchema = applyMappingToSchema(Target.VALUE, initialParentPath, record.valueSchema());
            mappedValue = copyValuesToNewSchema(Target.VALUE, initialParentPath, record.valueSchema(), mappedValueSchema, record.value());
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                mappedKeySchema,
                mappedKey,
                mappedValueSchema,
                mappedValue,
                record.timestamp()
        );
    }

    private Schema applyMappingToSchema(Target targetType, String parent, Schema source) {
        if (source == null) {
            return null;
        }

        return switch (source.type()) {
            case ARRAY -> {
                var mappedSchema = applyMappingToSchema(targetType, parent, source.valueSchema());
                var arrayBuilder = SchemaBuilder.array(mappedSchema).name(source.name());
                yield SchemaCopyUtil.copySchemaBasics(source, arrayBuilder).build();
            }
            case STRUCT -> applyMappingsToStruct(targetType, parent, source);
            case BYTES -> {
                if (Decimal.LOGICAL_NAME.equals(source.name())) {
                    yield normalizeDecimalSchema(targetType, parent, source);
                }

                yield source;
            }
            case null, default -> source;
        };
    }

    private Schema applyMappingsToStruct(Target targetType, String parent, Schema struct) {
        var copiedSchema = SchemaCopyUtil.copySchemaBasics(struct);

        for (var field : struct.fields()) {
            var nextField = "".equals(parent) ? field.name() : parent + "." + field.name();
            copiedSchema.field(field.name(), applyMappingToSchema(targetType, nextField, field.schema()));
        }

        return copiedSchema.build();
    }

    private Schema normalizeDecimalSchema(Target targetType, String parent, Schema decimal) {
        var shouldCopy = switch (targetType) {
            case VALUE -> valueFieldFilter.shouldApply(parent);
            case KEY -> keyFieldFilter.shouldApply(parent);
        };

        if (!shouldCopy) {
            return decimal;
        }

        var newDecimalSchema = SchemaCopyUtil.copySchemaBasics(decimal);
        var precisionAndScale = resolvePrecisionAndScale(decimal);

        newDecimalSchema.parameter(DECIMAL_PRECISION_PROPERTY, String.valueOf(precisionAndScale.precision));
        newDecimalSchema.parameter(DECIMAL_SCALE_PROPERTY, String.valueOf(precisionAndScale.scale));

        if (decimal.isOptional()) {
            newDecimalSchema.optional();
        }

        return newDecimalSchema.build();
    }

    private Object copyValuesToNewSchema(Target targetType, String parent, Schema source, Schema target, Object input) {
        if (input == null) {
            return null;
        }

        return switch (source.type()) {
            case ARRAY -> copyArray(targetType, parent, source.valueSchema(), target.valueSchema(), input);
            case STRUCT -> copyStruct(targetType, parent, source, target, input);
            case BYTES -> {
                if (Decimal.LOGICAL_NAME.equals(source.name())) {
                    yield copyDecimal(targetType, parent, source, target, input);
                }

                yield source;
            }
            case null, default -> input;
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> copyArray(Target targetType, String parent, Schema source, Schema target, Object input) {
        var inputObjects = (List<Object>) input;

        return inputObjects.stream().map(it -> copyValuesToNewSchema(targetType, parent, source, target, it)).toList();
    }

    private Struct copyStruct(Target targetType, String parent, Schema source, Schema target, Object input) {
        var currentStruct = requireStruct(input, "struct required");
        var newStruct = new Struct(target);

        for (var field : source.fields()) {
            var currentValue = currentStruct.get(field);
            var currentSchema = field.schema();
            var targetSchema = target.field(field.name()).schema();

            var nextField = "".equals(parent) ? field.name() : parent + "." + field.name();

            newStruct.put(field, copyValuesToNewSchema(targetType, nextField, currentSchema, targetSchema, currentValue));
        }

        return newStruct;
    }

    private Object copyDecimal(Target targetType, String parent, Schema source, Schema target, Object input) {
        var shouldCopy = switch (targetType) {
            case VALUE -> valueFieldFilter.shouldApply(parent);
            case KEY -> keyFieldFilter.shouldApply(parent);
        };

        if (!shouldCopy) {
            return input;
        }

        var currentDecimal = (BigDecimal) input;

        var updatedScale = Integer.parseInt(target.parameters().getOrDefault(DECIMAL_SCALE_PROPERTY, String.valueOf(undefinedScale)));
        var updatedPrecision = Integer.parseInt(target.parameters().getOrDefault(DECIMAL_PRECISION_PROPERTY, String.valueOf(undefinedPrecision)));

        var mathContext = new MathContext(updatedPrecision);
        return currentDecimal.round(mathContext).setScale(updatedScale, RoundingMode.FLOOR);
    }

    private PrecisionAndScale resolvePrecisionAndScale(Schema decimal) {
        var currentPrecision = Integer.parseInt(decimal.parameters().getOrDefault(DECIMAL_PRECISION_PROPERTY, String.valueOf(undefinedPrecision)));
        var currentScale = Integer.parseInt(decimal.parameters().getOrDefault(DECIMAL_SCALE_PROPERTY, String.valueOf(undefinedScale)));

        var isPrecisionUndefined = undefinedPrecision == currentPrecision;
        var isPrecisionExceeded = currentPrecision > precision;

        var isScaleUndefined = undefinedScale == currentScale;
        var isScaleExceeded = currentScale > scale;
        var isScaleZero = currentScale == 0;
        var isScaleNegative = currentScale < 0;

        // set precision if:
        // mode is IF_NOT_SET and currentPrecision is undefined
        // mode is LIMIT and currentPrecision > target precision
        // mode is VALUE
        var shouldSetPrecision =
                (PrecisionMode.IF_NOT_SET.equals(precisionMode) && isPrecisionUndefined)
                        || (PrecisionMode.LIMIT.equals(precisionMode) && isPrecisionExceeded)
                        || (PrecisionMode.VALUE.equals(precisionMode));

        // set scale if:
        // mode is IF_NOT_SET and currentScale is undefined
        // mode is LIMIT and currentScale > target scale
        // mode is VALUE
        var shouldSetScale =
                (ScaleMode.IF_NOT_SET.equals(scaleMode) && isScaleUndefined)
                        || (ScaleMode.LIMIT.equals(scaleMode) && isScaleExceeded)
                        || (ScaleMode.VALUE.equals(scaleMode))
                        || (ScaleNegativeMode.VALUE.equals(scaleNegativeMode) && isScaleNegative);

        var shouldSetScaleToZero = (ScaleZeroMode.VALUE.equals(scaleZeroMode) && isScaleZero);

        var targetPrecision = currentPrecision;
        var targetScale = currentScale;

        if (shouldSetPrecision) {
            targetPrecision = precision;
        }

        if (shouldSetScale) {
            targetScale = scale;
        } else if (shouldSetScaleToZero) {
            targetScale = 0;
        }

        return new PrecisionAndScale(targetPrecision, targetScale);
    }
}
