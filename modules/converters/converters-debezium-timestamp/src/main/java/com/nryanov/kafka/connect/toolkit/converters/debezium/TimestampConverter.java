package com.nryanov.kafka.connect.toolkit.converters.debezium;

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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    public static final Set<String> SUPPORTED_DATA_TYPES = Set.of("date", "timestamp", "timestamptz", "time", "timetz");

    private static final DateTimeFormatter DATETIME_WITHOUT_TIMEZONE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private static final DateTimeFormatter TIME_WITHOUT_TIMEZONE_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final DateTimeFormatter TIME_WITH_TIMEZONE_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss[.][SSSSSSSSS][SSSSSSS][SSSSSS][SSSSS][SSSS][SSS][SS][S][''][XXX][XX][X]");

    @Override
    public void configure(Properties props) {}

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (SUPPORTED_DATA_TYPES.contains(column.typeName().toLowerCase())) {
            switch (column.typeName()) {
                case "timestamptz" -> convertTimestampTz(column, registration);
                case "timestamp" -> convertTimestamp(column, registration);
                case "date" -> convertDate(column, registration);
                case "time" -> convertTime(column, registration);
                case "timetz" -> convertTimeTz(column, registration);
                default -> {}
            }
        }
    }

    private void convertTimestamp(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var builder = SchemaBuilder.string().optional();

        registration.register(builder, rawValue -> {
            if (rawValue == null) {
                return null;
            }

            return switch (rawValue) {
                case String str -> {
                    var timestamp = LocalDateTime.parse(str);
                    yield DATETIME_WITHOUT_TIMEZONE_FORMATTER.format(timestamp);
                }
                case java.sql.Timestamp ts -> DATETIME_WITHOUT_TIMEZONE_FORMATTER.format(ts.toInstant().atOffset(ZoneOffset.UTC));
                case LocalDateTime ldt -> DATETIME_WITHOUT_TIMEZONE_FORMATTER.format(ldt);
                case Instant odt -> DATETIME_WITHOUT_TIMEZONE_FORMATTER.format(odt.atOffset(ZoneOffset.UTC));
                case Long l -> DATETIME_WITHOUT_TIMEZONE_FORMATTER.format(LocalDateTime.ofEpochSecond(l, 0, ZoneOffset.UTC));
                default -> throw new IllegalArgumentException("Unexpected type for timestamp: " + rawValue.getClass().getName() + " in column " + column.name());
            };
        });
    }


    private void convertTimestampTz(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var builder = Timestamp.builder().optional();

        registration.register(builder, rawValue -> {
            if (rawValue == null) {
                return null;
            }

            return switch (rawValue) {
                case String ignored -> {
                    var timestamp = OffsetDateTime.parse(rawValue.toString());
                    yield  Date.from(timestamp.toInstant());
                }
                case java.sql.Timestamp ts -> Date.from(ts.toInstant());
                case java.time.ZonedDateTime zdt -> Date.from(zdt.toInstant());
                case OffsetDateTime odt -> Date.from(odt.toInstant());
                case Instant odt -> Date.from(odt);
                default -> throw new IllegalArgumentException("Unexpected type for timestamptz: " + rawValue.getClass().getName() + " in column " + column.name());
            };
        });
    }

    private void convertDate(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var builder = org.apache.kafka.connect.data.Date.builder().optional();

        registration.register(builder, rawValue -> {
            if (rawValue == null) {
                return null;
            }

            return switch (rawValue) {
                case String ignored -> {
                    var timestamp = OffsetDateTime.parse(rawValue.toString());
                    yield  Date.from(timestamp.toInstant());
                }
                case Integer epochDays -> Date.from(LocalDate.ofEpochDay(epochDays).atStartOfDay(ZoneOffset.UTC).toInstant());
                case java.sql.Date d -> Date.from(d.toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant());
                case LocalDate ld -> Date.from(ld.atStartOfDay(ZoneOffset.UTC).toInstant());
                default -> throw new IllegalArgumentException("Unexpected type for date: " + rawValue.getClass().getName() + " in column " + column.name());
            };
        });
    }

    private void convertTime(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var builder = SchemaBuilder.string().optional();

        registration.register(builder, rawValue -> {
            if (rawValue == null) {
                return null;
            }

            return switch (rawValue) {
                case String str -> {
                    var timestamp = LocalTime.parse(str);
                    yield TIME_WITHOUT_TIMEZONE_FORMATTER.format(timestamp);
                }
                case java.sql.Time time -> TIME_WITHOUT_TIMEZONE_FORMATTER.format(time.toLocalTime());
                case LocalTime lt -> TIME_WITHOUT_TIMEZONE_FORMATTER.format(lt);
                case Instant odt -> TIME_WITHOUT_TIMEZONE_FORMATTER.format(odt.atOffset(ZoneOffset.UTC));
                case Long l -> TIME_WITHOUT_TIMEZONE_FORMATTER.format(LocalTime.ofSecondOfDay(l));
                default -> throw new IllegalArgumentException("Unexpected type for time: " + rawValue.getClass().getName() + " in column " + column.name());
            };
        });
    }

    private void convertTimeTz(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        var builder = org.apache.kafka.connect.data.Time.builder().optional();

        registration.register(builder, rawValue -> {
            if (rawValue == null) {
                return null;
            }

            return switch (rawValue) {
                case String str -> {
                    var timestamp = OffsetTime.parse(str, TIME_WITH_TIMEZONE_FORMATTER);
                    yield  Date.from(Instant.ofEpochMilli(timestamp.get(ChronoField.MILLI_OF_DAY)));
                }
                case OffsetTime ot -> Date.from(Instant.ofEpochMilli(ot.get(ChronoField.MILLI_OF_DAY)));
                default -> throw new IllegalArgumentException("Unexpected type for timetz: " + rawValue.getClass().getName() + " in column " + column.name());
            };
        });
    }
}
