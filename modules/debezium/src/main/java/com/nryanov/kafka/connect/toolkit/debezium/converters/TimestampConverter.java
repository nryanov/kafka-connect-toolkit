package com.nryanov.kafka.connect.toolkit.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;

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
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    enum TimestampType {
        STRING(() -> SchemaBuilder.string().optional()),
        TIMESTAMP(() -> Timestamp.builder().optional());

        public final Supplier<SchemaBuilder> builder;

        TimestampType(Supplier<SchemaBuilder> builder) {
            this.builder = builder;
        }
    }

    enum TimestampTzType {
        STRING(() -> SchemaBuilder.string().optional()),
        TIMESTAMP(() -> Timestamp.builder().optional());

        public final Supplier<SchemaBuilder> builder;

        TimestampTzType(Supplier<SchemaBuilder> builder) {
            this.builder = builder;
        }
    }

    enum DateType {
        STRING(() -> SchemaBuilder.string().optional()),
        DATE(() -> org.apache.kafka.connect.data.Date.builder().optional());

        public final Supplier<SchemaBuilder> builder;

        DateType(Supplier<SchemaBuilder> builder) {
            this.builder = builder;
        }
    }

    enum TimeType {
        STRING(() -> SchemaBuilder.string().optional()),
        TIME(() -> org.apache.kafka.connect.data.Time.builder().optional());

        public final Supplier<SchemaBuilder> builder;

        TimeType(Supplier<SchemaBuilder> builder) {
            this.builder = builder;
        }
    }

    enum TimeTzType {
        STRING(() -> SchemaBuilder.string().optional()),
        TIME(() -> org.apache.kafka.connect.data.Time.builder().optional());

        public final Supplier<SchemaBuilder> builder;

        TimeTzType(Supplier<SchemaBuilder> builder) {
            this.builder = builder;
        }
    }

    public static final Set<String> SUPPORTED_DATA_TYPES = Set.of("date", "timestamp", "timestamptz", "time", "timetz");

    private static final ZoneId UTC = ZoneId.of("UTC");

    private static final DateTimeFormatter DATETIME_WITHOUT_TIMEZONE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private static final DateTimeFormatter DATETIME_WITH_TIMEZONE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(UTC);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(UTC);
    private static final DateTimeFormatter TIME_WITHOUT_TIMEZONE_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final DateTimeFormatter TIME_WITH_TIMEZONE_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS'Z'").withZone(UTC);

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
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (SUPPORTED_DATA_TYPES.contains(column.typeName().toLowerCase())) {
            switch (column.typeName()) {
                case "timestamptz" -> convertTimestampTz(column, registration);
                case "timestamp" -> convertTimestamp(column, registration);
                case "date" -> convertDate(column, registration);
                case "time" -> convertTime(column, registration);
                case "timetz" -> convertTimeTz(column, registration);
                default -> {
                }
            }
        }
    }

    private void convertTimestamp(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var builder = timestampType.builder.get();

        registration.register(builder, rawValue -> {
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
                            DATETIME_WITHOUT_TIMEZONE_FORMATTER.format(ts.toInstant().atOffset(ZoneOffset.UTC).plusSeconds(timestampShift.getTotalSeconds()));
                    case LocalDateTime ldt ->
                            DATETIME_WITHOUT_TIMEZONE_FORMATTER.format(ldt.plusSeconds(timestampShift.getTotalSeconds()));
                    case Instant odt ->
                            DATETIME_WITHOUT_TIMEZONE_FORMATTER.format(odt.atOffset(ZoneOffset.UTC).plusSeconds(timestampShift.getTotalSeconds()));
                    case Long l ->
                            DATETIME_WITHOUT_TIMEZONE_FORMATTER.format(LocalDateTime.ofEpochSecond(l, 0, ZoneOffset.UTC).plusSeconds(timestampShift.getTotalSeconds()));
                    default ->
                            throw new IllegalArgumentException("Unexpected type for timestamp: " + rawValue.getClass().getName() + " in column " + column.name());
                };
            };
        });
    }


    private void convertTimestampTz(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var builder = timestampTzType.builder.get();

        registration.register(builder, rawValue -> {
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
                        yield DATETIME_WITH_TIMEZONE_FORMATTER.format(timestamp);
                    }
                    case java.sql.Timestamp ts -> DATETIME_WITH_TIMEZONE_FORMATTER.format(ts.toInstant());
                    case java.time.ZonedDateTime zdt -> DATETIME_WITH_TIMEZONE_FORMATTER.format(zdt.toInstant());
                    case OffsetDateTime odt -> DATETIME_WITH_TIMEZONE_FORMATTER.format(odt.toInstant());
                    case Instant odt -> DATETIME_WITH_TIMEZONE_FORMATTER.format(odt);
                    default ->
                            throw new IllegalArgumentException("Unexpected type for timestamptz: " + rawValue.getClass().getName() + " in column " + column.name());
                };
            };
        });
    }

    private void convertDate(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var builder = dateType.builder.get();

        registration.register(builder, rawValue -> {
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
                        yield DATE_FORMATTER.format(timestamp.toInstant());
                    }
                    case Integer epochDays ->
                            DATE_FORMATTER.format(LocalDate.ofEpochDay(epochDays).atStartOfDay(ZoneOffset.UTC).toInstant());
                    case java.sql.Date d ->
                            DATE_FORMATTER.format(d.toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant());
                    case LocalDate ld -> DATE_FORMATTER.format(ld.atStartOfDay(ZoneOffset.UTC).toInstant());
                    default ->
                            throw new IllegalArgumentException("Unexpected type for date: " + rawValue.getClass().getName() + " in column " + column.name());
                };
            };
        });
    }

    private void convertTime(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var builder = timeType.builder.get();

        registration.register(builder, rawValue -> {
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
                        yield TIME_WITHOUT_TIMEZONE_FORMATTER.format(timestamp);
                    }
                    case java.sql.Time time ->
                            TIME_WITHOUT_TIMEZONE_FORMATTER.format(time.toLocalTime().plusSeconds(timeShift.getTotalSeconds()));
                    case LocalTime lt ->
                            TIME_WITHOUT_TIMEZONE_FORMATTER.format(lt.plusSeconds(timeShift.getTotalSeconds()));
                    case Instant odt ->
                            TIME_WITHOUT_TIMEZONE_FORMATTER.format(odt.atOffset(ZoneOffset.UTC).plusSeconds(timeShift.getTotalSeconds()));
                    case Long l ->
                            TIME_WITHOUT_TIMEZONE_FORMATTER.format(LocalTime.ofSecondOfDay(l).plusSeconds(timeShift.getTotalSeconds()));
                    default ->
                            throw new IllegalArgumentException("Unexpected type for time: " + rawValue.getClass().getName() + " in column " + column.name());
                };
            };
        });
    }

    private void convertTimeTz(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var builder = timeTzType.builder.get();

        registration.register(builder, rawValue -> {
            if (rawValue == null) {
                return null;
            }

            return switch (timeTzType) {
                case TIME -> switch (rawValue) {
                    case String str -> {
                        var timestamp = OffsetTime.parse(str, TIME_PARSER).withOffsetSameInstant(ZoneOffset.UTC);
                        yield Date.from(Instant.ofEpochMilli(timestamp.get(ChronoField.MILLI_OF_DAY)));
                    }
                    case OffsetTime ot -> Date.from(Instant.ofEpochMilli(ot.get(ChronoField.MILLI_OF_DAY)));
                    default ->
                            throw new IllegalArgumentException("Unexpected type for timetz: " + rawValue.getClass().getName() + " in column " + column.name());
                };
                case STRING -> switch (rawValue) {
                    case String str -> {
                        var timestamp = OffsetTime.parse(str, TIME_PARSER).withOffsetSameInstant(ZoneOffset.UTC);
                        yield TIME_WITH_TIMEZONE_FORMATTER.format(timestamp);
                    }
                    case OffsetTime ot -> TIME_WITH_TIMEZONE_FORMATTER.format(ot);
                    default ->
                            throw new IllegalArgumentException("Unexpected type for timetz: " + rawValue.getClass().getName() + " in column " + column.name());
                };
            };
        });
    }
}
