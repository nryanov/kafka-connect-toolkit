package com.nryanov.kafka.connect.toolkit.debezium.postgres.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.postgresql.jdbc.PgArray;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    enum TimestampType {
        STRING(() -> SchemaBuilder.string().optional(), () -> SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional()),
        TIMESTAMP(() -> Timestamp.builder().optional(), () -> SchemaBuilder.array(Timestamp.builder().optional().build()).optional());

        public final Supplier<SchemaBuilder> builder;
        public final Supplier<SchemaBuilder> arrayBuilder;

        TimestampType(
                Supplier<SchemaBuilder> builder,
                Supplier<SchemaBuilder> arrayBuilder
        ) {
            this.builder = builder;
            this.arrayBuilder = arrayBuilder;
        }
    }

    enum TimestampTzType {
        STRING(() -> SchemaBuilder.string().optional(), () -> SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional()),
        TIMESTAMP(() -> Timestamp.builder().optional(), () -> SchemaBuilder.array(Timestamp.builder().optional().build()).optional());

        public final Supplier<SchemaBuilder> builder;
        public final Supplier<SchemaBuilder> arrayBuilder;

        TimestampTzType(
                Supplier<SchemaBuilder> builder,
                Supplier<SchemaBuilder> arrayBuilder
        ) {
            this.builder = builder;
            this.arrayBuilder = arrayBuilder;
        }
    }

    enum DateType {
        STRING(() -> SchemaBuilder.string().optional(), () -> SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional()),
        DATE(() -> org.apache.kafka.connect.data.Date.builder().optional(), () -> SchemaBuilder.array(org.apache.kafka.connect.data.Date.builder().optional().build()).optional());

        public final Supplier<SchemaBuilder> builder;
        public final Supplier<SchemaBuilder> arrayBuilder;

        DateType(
                Supplier<SchemaBuilder> builder,
                Supplier<SchemaBuilder> arrayBuilder
        ) {
            this.builder = builder;
            this.arrayBuilder = arrayBuilder;
        }
    }

    enum TimeType {
        STRING(() -> SchemaBuilder.string().optional(), () -> SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional()),
        TIME(() -> org.apache.kafka.connect.data.Time.builder().optional(), () -> SchemaBuilder.array(org.apache.kafka.connect.data.Time.builder().optional().build()).optional());

        public final Supplier<SchemaBuilder> builder;
        public final Supplier<SchemaBuilder> arrayBuilder;

        TimeType(
                Supplier<SchemaBuilder> builder,
                Supplier<SchemaBuilder> arrayBuilder
        ) {
            this.builder = builder;
            this.arrayBuilder = arrayBuilder;
        }
    }

    enum TimeTzType {
        STRING(() -> SchemaBuilder.string().optional(), () -> SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional()),
        TIME(() -> org.apache.kafka.connect.data.Time.builder().optional(), () -> SchemaBuilder.array(org.apache.kafka.connect.data.Time.builder().optional().build()).optional());

        public final Supplier<SchemaBuilder> builder;
        public final Supplier<SchemaBuilder> arrayBuilder;

        TimeTzType(
                Supplier<SchemaBuilder> builder,
                Supplier<SchemaBuilder> arrayBuilder
        ) {
            this.builder = builder;
            this.arrayBuilder = arrayBuilder;
        }
    }

    public static final Set<String> SUPPORTED_TEMPORAL_TYPES = Set.of("date", "timestamp", "timestamptz", "time", "timetz");
    public static final Set<String> SUPPORTED_TEMPORAL_ARRAY_TYPES = Set.of("_date", "_timestamp", "_timestamptz", "_time", "_timetz");

    private static final ZoneId UTC = ZoneId.of("UTC");

    private static final String DEFAULT_TIMESTAMP_WITHOUT_TIMEZONE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    private static final String DEFAULT_TIMESTAMP_WITH_TIMEZONE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd";
    private static final String DEFAULT_TIMES_WITHOUT_TIMEZONE_PATTERN = "HH:mm:ss.SSS";
    private static final String DEFAULT_TIMES_WITH_TIMEZONE_PATTERN = "HH:mm:ss.SSS'Z'";

    private DateTimeFormatter timestampWithoutTimezoneFormatter;
    private DateTimeFormatter timestampWithTimezoneFormatter;
    private DateTimeFormatter dateFormatter;
    private DateTimeFormatter timeWithoutTimezoneFormatter;
    private DateTimeFormatter timeWithTimezoneFormatter;

    private static final DateTimeFormatter TIME_PARSER = DateTimeFormatter.ofPattern("HH:mm:ss[.][SSSSSSSSS][SSSSSSS][SSSSSS][SSSSS][SSSS][SSS][SS][S][''][XXX][XX][X]");

    private ZoneOffset timestampShift;
    private ZoneOffset timeShift;

    private TimestampType timestampType;
    private TimestampTzType timestampTzType;
    private DateType dateType;
    private TimeType timeType;
    private TimeTzType timeTzType;

    @Override
    public void configure(Properties props) {
        var timestampShiftRaw = props.getProperty("timestamp.shift", "Z");
        var timeShiftRaw = props.getProperty("time.shift", "Z");

        this.timestampType = TimestampType.valueOf(props.getProperty("timestamp.type", "STRING"));
        this.timestampTzType = TimestampTzType.valueOf(props.getProperty("timestamptz.type", "TIMESTAMP"));
        this.dateType = DateType.valueOf(props.getProperty("date.type", "DATE"));
        this.timeType = TimeType.valueOf(props.getProperty("time.type", "STRING"));
        this.timeTzType = TimeTzType.valueOf(props.getProperty("timetz.type", "TIME"));

        this.timestampShift = ZoneOffset.of(timestampShiftRaw);
        this.timeShift = ZoneOffset.of(timeShiftRaw);

        var timestampWithoutTimezonePattern = props.getProperty("timestamp.pattern", DEFAULT_TIMESTAMP_WITHOUT_TIMEZONE_PATTERN);
        var timestampWithTimezonePattern = props.getProperty("timestamptz.pattern", DEFAULT_TIMESTAMP_WITH_TIMEZONE_PATTERN);
        var datePattern = props.getProperty("date.pattern", DEFAULT_DATE_PATTERN);
        var timeWithoutTimezonePattern = props.getProperty("time.pattern", DEFAULT_TIMES_WITHOUT_TIMEZONE_PATTERN);
        var timeWithTimezonePattern = props.getProperty("timetz.pattern", DEFAULT_TIMES_WITH_TIMEZONE_PATTERN);

        this.timestampWithoutTimezoneFormatter = DateTimeFormatter.ofPattern(timestampWithoutTimezonePattern).withZone(UTC);
        this.timestampWithTimezoneFormatter = DateTimeFormatter.ofPattern(timestampWithTimezonePattern).withZone(UTC);
        this.dateFormatter = DateTimeFormatter.ofPattern(datePattern).withZone(UTC);
        this.timeWithoutTimezoneFormatter = DateTimeFormatter.ofPattern(timeWithoutTimezonePattern).withZone(UTC);
        this.timeWithTimezoneFormatter = DateTimeFormatter.ofPattern(timeWithTimezonePattern).withZone(UTC);
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (SUPPORTED_TEMPORAL_TYPES.contains(column.typeName().toLowerCase())) {
            registerConverterForPlainTemporalType(column, registration);
        } else if (SUPPORTED_TEMPORAL_ARRAY_TYPES.contains(column.typeName().toLowerCase())) {
            registerConverterForArrayTemporalType(column, registration);
        }
    }

    private void registerConverterForArrayTemporalType(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        switch (column.typeName().toLowerCase()) {
            case "_timestamp" -> convertArrayTimestamp(column, registration);
            case "_timestamptz" -> convertArrayTimestampTz(column, registration);
            case "_date" -> convertArrayDate(column, registration);
            case "_time" -> convertArrayTime(column, registration);
            case "_timetz" -> convertArrayTimeTz(column, registration);
            default -> {
            }
        }
    }

    private void registerConverterForPlainTemporalType(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        switch (column.typeName().toLowerCase()) {
            case "timestamptz" -> convertTimestampTz(column, registration);
            case "timestamp" -> convertTimestamp(column, registration);
            case "date" -> convertDate(column, registration);
            case "time" -> convertTime(column, registration);
            case "timetz" -> convertTimeTz(column, registration);
            default -> {
            }
        }
    }

    private void convertArrayTimestamp(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var schema = timestampType.arrayBuilder.get();
        registration.register(schema, rawValue -> Arrays.stream(getArray(rawValue)).map(it -> it == null ? null : parseTimestamp(column, it)).toList());
    }

    private void convertArrayTimestampTz(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var schema = timestampTzType.arrayBuilder.get();
        registration.register(schema, rawValue -> Arrays.stream(getArray(rawValue)).map(it -> it == null ? null : parseTimestampTz(column, it)).toList());
    }

    private void convertArrayDate(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var schema = dateType.arrayBuilder.get();
        registration.register(schema, rawValue -> Arrays.stream(getArray(rawValue)).map(it -> it == null ? null : parseDate(column, it)).toList());
    }

    private void convertArrayTime(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var schema = timeType.arrayBuilder.get();
        registration.register(schema, rawValue -> Arrays.stream(getArray(rawValue)).map(it -> it == null ? null : parseTime(column, it)).toList());
    }

    private void convertArrayTimeTz(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var schema = timeTzType.arrayBuilder.get();
        registration.register(schema, rawValue -> Arrays.stream(getArray(rawValue)).map(it -> it == null ? null : parseTimeTz(column, it)).toList());
    }

    private void convertTimestamp(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var schema = timestampType.builder.get();
        registration.register(schema, rawValue -> parseTimestamp(column, rawValue));
    }

    private void convertTimestampTz(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var schema = timestampTzType.builder.get();
        registration.register(schema, rawValue -> parseTimestampTz(column, rawValue));
    }

    private void convertDate(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var schema = dateType.builder.get();
        registration.register(schema, rawValue -> parseDate(column, rawValue));
    }

    private void convertTime(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var schema = timeType.builder.get();
        registration.register(schema, rawValue -> parseTime(column, rawValue));
    }

    private void convertTimeTz(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var schema = timeTzType.builder.get();
        registration.register(schema, rawValue -> parseTimeTz(column, rawValue));
    }

    private Object parseTimestamp(RelationalColumn column, Object rawValue) {
        if (rawValue == null) {
            return null;
        }

        return switch (timestampType) {
            case TIMESTAMP -> switch (rawValue) {
                case java.sql.Timestamp ts ->
                        Date.from(ts.toInstant().atOffset(ZoneOffset.UTC).plusSeconds(timestampShift.getTotalSeconds()).toInstant());
                case LocalDateTime ldt ->
                        Date.from(ldt.atOffset(ZoneOffset.UTC).plusSeconds(timestampShift.getTotalSeconds()).toInstant());
                case Instant odt ->
                        Date.from(odt.atOffset(ZoneOffset.UTC).plusSeconds(timestampShift.getTotalSeconds()).toInstant());
                case Long l ->
                        Date.from(LocalDateTime.ofEpochSecond(l, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC).plusSeconds(timestampShift.getTotalSeconds()).toInstant());
                default ->
                        throw new IllegalArgumentException("Unexpected type for timestamp: " + rawValue.getClass().getName() + " in column " + column.name());
            };
            case STRING -> switch (rawValue) {
                case java.sql.Timestamp ts ->
                        timestampWithoutTimezoneFormatter.format(ts.toInstant().atOffset(ZoneOffset.UTC).plusSeconds(timestampShift.getTotalSeconds()));
                case LocalDateTime ldt ->
                        timestampWithoutTimezoneFormatter.format(ldt.plusSeconds(timestampShift.getTotalSeconds()));
                case Instant odt ->
                        timestampWithoutTimezoneFormatter.format(odt.atOffset(ZoneOffset.UTC).plusSeconds(timestampShift.getTotalSeconds()));
                case Long l ->
                        timestampWithoutTimezoneFormatter.format(LocalDateTime.ofEpochSecond(l, 0, ZoneOffset.UTC).plusSeconds(timestampShift.getTotalSeconds()));
                default ->
                        throw new IllegalArgumentException("Unexpected type for timestamp: " + rawValue.getClass().getName() + " in column " + column.name());
            };
        };
    }

    private Object parseTimestampTz(RelationalColumn column, Object rawValue) {
        if (rawValue == null) {
            return null;
        }

        return switch (timestampTzType) {
            case TIMESTAMP -> switch (rawValue) {
                case String ignored -> {
                    var timestamp = OffsetDateTime.parse(rawValue.toString());
                    yield Date.from(timestamp.toInstant());
                }
                case java.sql.Timestamp ts -> Date.from(ts.toInstant());
                case java.time.ZonedDateTime zdt -> Date.from(zdt.toInstant());
                case OffsetDateTime odt -> Date.from(odt.toInstant());
                case Instant odt -> Date.from(odt);
                default ->
                        throw new IllegalArgumentException("Unexpected type for timestamptz: " + rawValue.getClass().getName() + " in column " + column.name());
            };
            case STRING -> switch (rawValue) {
                case String ignored -> {
                    var timestamp = OffsetDateTime.parse(rawValue.toString());
                    yield timestampWithTimezoneFormatter.format(timestamp);
                }
                case java.sql.Timestamp ts -> timestampWithTimezoneFormatter.format(ts.toInstant());
                case java.time.ZonedDateTime zdt -> timestampWithTimezoneFormatter.format(zdt.toInstant());
                case OffsetDateTime odt -> timestampWithTimezoneFormatter.format(odt.toInstant());
                case Instant odt -> timestampWithTimezoneFormatter.format(odt);
                default ->
                        throw new IllegalArgumentException("Unexpected type for timestamptz: " + rawValue.getClass().getName() + " in column " + column.name());
            };
        };
    }

    private Object parseDate(RelationalColumn column, Object rawValue) {
        if (rawValue == null) {
            return null;
        }

        return switch (dateType) {
            case DATE -> switch (rawValue) {
                case String ignored -> {
                    var timestamp = OffsetDateTime.parse(rawValue.toString());
                    yield Date.from(timestamp.toInstant());
                }
                case Integer epochDays ->
                        Date.from(LocalDate.ofEpochDay(epochDays).atStartOfDay(ZoneOffset.UTC).toInstant());
                case java.sql.Date d -> Date.from(d.toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant());
                case LocalDate ld -> Date.from(ld.atStartOfDay(ZoneOffset.UTC).toInstant());
                default ->
                        throw new IllegalArgumentException("Unexpected type for date: " + rawValue.getClass().getName() + " in column " + column.name());
            };
            case STRING -> switch (rawValue) {
                case String ignored -> {
                    var timestamp = OffsetDateTime.parse(rawValue.toString());
                    yield dateFormatter.format(timestamp.toInstant());
                }
                case Integer epochDays ->
                        dateFormatter.format(LocalDate.ofEpochDay(epochDays).atStartOfDay(ZoneOffset.UTC).toInstant());
                case java.sql.Date d -> dateFormatter.format(d.toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant());
                case LocalDate ld -> dateFormatter.format(ld.atStartOfDay(ZoneOffset.UTC).toInstant());
                default ->
                        throw new IllegalArgumentException("Unexpected type for date: " + rawValue.getClass().getName() + " in column " + column.name());
            };
        };
    }

    private Object parseTime(RelationalColumn column, Object rawValue) {
        if (rawValue == null) {
            return null;
        }

        return switch (timeType) {
            case TIME -> switch (rawValue) {
                case String str -> {
                    var timestamp = LocalTime.parse(str, TIME_PARSER).plusSeconds(timeShift.getTotalSeconds());
                    yield Date.from(Instant.ofEpochMilli(timestamp.get(ChronoField.MILLI_OF_DAY)));
                }
                case java.sql.Time time ->
                        Date.from(Instant.ofEpochMilli(time.toLocalTime().plusSeconds(timeShift.getTotalSeconds()).get(ChronoField.MILLI_OF_DAY)));
                case LocalTime lt ->
                        Date.from(Instant.ofEpochMilli(lt.plusSeconds(timeShift.getTotalSeconds()).get(ChronoField.MILLI_OF_DAY)));
                case Instant odt ->
                        Date.from(Instant.ofEpochMilli(odt.atOffset(ZoneOffset.UTC).plusSeconds(timeShift.getTotalSeconds()).get(ChronoField.MILLI_OF_DAY)));
                case Long l ->
                        Date.from(Instant.ofEpochMilli(LocalTime.ofSecondOfDay(l).plusSeconds(timeShift.getTotalSeconds()).get(ChronoField.MILLI_OF_DAY)));
                default ->
                        throw new IllegalArgumentException("Unexpected type for time: " + rawValue.getClass().getName() + " in column " + column.name());
            };
            case STRING -> switch (rawValue) {
                case String str -> {
                    var timestamp = LocalTime.parse(str, TIME_PARSER).plusSeconds(timeShift.getTotalSeconds());
                    yield timeWithoutTimezoneFormatter.format(timestamp);
                }
                case java.sql.Time time ->
                        timeWithoutTimezoneFormatter.format(time.toLocalTime().plusSeconds(timeShift.getTotalSeconds()));
                case LocalTime lt -> timeWithoutTimezoneFormatter.format(lt.plusSeconds(timeShift.getTotalSeconds()));
                case Instant odt ->
                        timeWithoutTimezoneFormatter.format(odt.atOffset(ZoneOffset.UTC).plusSeconds(timeShift.getTotalSeconds()));
                case Long l ->
                        timeWithoutTimezoneFormatter.format(LocalTime.ofSecondOfDay(l).plusSeconds(timeShift.getTotalSeconds()));
                default ->
                        throw new IllegalArgumentException("Unexpected type for time: " + rawValue.getClass().getName() + " in column " + column.name());
            };
        };
    }

    private Object parseTimeTz(RelationalColumn column, Object rawValue) {
        if (rawValue == null) {
            return null;
        }

        return switch (timeTzType) {
            case TIME -> switch (rawValue) {
                case String str -> {
                    var timestamp = OffsetTime.parse(str, TIME_PARSER).withOffsetSameInstant(ZoneOffset.UTC);
                    yield Date.from(Instant.ofEpochMilli(timestamp.get(ChronoField.MILLI_OF_DAY)));
                }
                case java.sql.Time time -> Date.from(Instant.ofEpochMilli(time.toLocalTime().get(ChronoField.MILLI_OF_DAY)));
                case OffsetTime ot -> Date.from(Instant.ofEpochMilli(ot.get(ChronoField.MILLI_OF_DAY)));
                default ->
                        throw new IllegalArgumentException("Unexpected type for timetz: " + rawValue.getClass().getName() + " in column " + column.name());
            };
            case STRING -> switch (rawValue) {
                case String str -> {
                    var timestamp = OffsetTime.parse(str, TIME_PARSER).withOffsetSameInstant(ZoneOffset.UTC);
                    yield timeWithTimezoneFormatter.format(timestamp);
                }
                case java.sql.Time time -> timeWithTimezoneFormatter.format(time.toLocalTime());
                case OffsetTime ot -> timeWithTimezoneFormatter.format(ot);
                default ->
                        throw new IllegalArgumentException("Unexpected type for timetz: " + rawValue.getClass().getName() + " in column " + column.name());
            };
        };
    }

    private Object[] getArray(final Object x) {
        try {
            return (Object[]) ((PgArray) x).getArray();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
