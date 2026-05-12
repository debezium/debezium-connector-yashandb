/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Time;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yashandb.jdbc.YasTypes;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.ValueConverter;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

@ThreadSafe
@Immutable
public class YashanDBDefaultValueConverter implements DefaultValueConverter {

    private static Logger LOGGER = LoggerFactory.getLogger(YashanDBDefaultValueConverter.class);

    private final YashanDBValueConverters valueConverters;
    private final Map<Integer, DefaultValueMapper> defaultValueMappers;
    private static final Pattern TIME_FIELD_PATTERN = Pattern.compile("(\\-?[0-9]*):([0-9]*)(:([0-9]*))?(\\.([0-9]*))?");
    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("([0-9]*-[0-9]*-[0-9]*) ([0-9]*:[0-9]*:[0-9]*(\\.([0-9]*))?)");
    private static final Pattern DATE_PATTERN = Pattern.compile("([0-9]*-[0-9]*-[0-9]*)");

    public YashanDBDefaultValueConverter(YashanDBValueConverters valueConverters, YashanDBConnection jdbcConnection) {
        this.valueConverters = valueConverters;
        this.defaultValueMappers = Collections.unmodifiableMap(createDefaultValueMappers(jdbcConnection));
    }

    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValue) {
        final int dataType = column.jdbcType();
        final DefaultValueMapper mapper = defaultValueMappers.get(dataType);
        if (mapper == null) {
            LOGGER.warn("Mapper for type '{}' not found.", dataType);
            return Optional.empty();
        }

        try {
            Object rawDefaultValue = mapper.parse(column, defaultValue != null ? defaultValue.trim() : defaultValue);
            Object convertedDefaultValue = convertDefaultValue(rawDefaultValue, column);
            if (convertedDefaultValue instanceof Struct) {
                // Workaround for KAFKA-12694
                LOGGER.warn("Struct can't be used as default value for column '{}', will use null instead.", column.name());
                return Optional.empty();
            }
            return Optional.ofNullable(convertedDefaultValue);
        }
        catch (Exception e) {
            LOGGER.warn("Cannot parse column default value '{}' to type '{}'.  Expression evaluation is not supported.", defaultValue, dataType);
            LOGGER.debug("Parsing failed due to error", e);
            return Optional.empty();
        }
    }

    private Object convertDefaultValue(Object defaultValue, Column column) {
        // if converters is not null and the default value is not null, we need to convert default value
        if (valueConverters != null && defaultValue != null) {
            final SchemaBuilder schemaBuilder = valueConverters.schemaBuilder(column);
            if (schemaBuilder != null) {
                final Schema schema = schemaBuilder.build();
                // In order to get the valueConverter for this column, we have to create a field;
                // The index value -1 in the field will never used when converting default value;
                // So we can set any number here;
                final Field field = new Field(column.name(), -1, schema);
                final ValueConverter valueConverter = valueConverters.converter(column, field);

                Object result = valueConverter.convert(defaultValue);
                if ((result instanceof BigDecimal) && column.scale().isPresent() && column.scale().get() > ((BigDecimal) result).scale()) {
                    // Note as the scale is increased only, the rounding is more cosmetic.
                    result = ((BigDecimal) result).setScale(column.scale().get(), RoundingMode.HALF_EVEN);
                }
                return result;
            }
        }
        return defaultValue;
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#DOUBLE}.
     *
     * @param value the string object to be converted into a {@link Types#DOUBLE} type;
     * @return the converted value;
     */
    private static Object convertToDouble(String value) {
        return Double.parseDouble(value);
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#DECIMAL}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param value the string object to be converted into a {@link Types#DECIMAL} type;
     * @return the converted value;
     */
    private static Object convertToDecimal(Column column, String value) {
        return column.scale().isPresent()
                ? new BigDecimal(value).setScale(column.scale().get(), RoundingMode.HALF_UP)
                : new BigDecimal(value);
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#BIT}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param value the string object to be converted into a {@link Types#BIT} type;
     * @return the converted value;
     */
    private Object convertToBits(Column column, String value) {
        if (column.length() > 1) {
            return convertToBits(value);
        }
        return convertToBit(value);
    }

    private Object convertToBit(String value) {
        try {
            return Short.parseShort(value) != 0;
        }
        catch (NumberFormatException ignore) {
            return Boolean.parseBoolean(value);
        }
    }

    private Object convertToBits(String value) {
        int nums = value.length() / Byte.SIZE + (value.length() % Byte.SIZE == 0 ? 0 : 1);
        byte[] bytes = new byte[nums];
        for (int i = 0; i < nums; i++) {
            int s = Math.max(value.length() - Byte.SIZE, 0);
            int e = value.length();
            bytes[nums - i - 1] = (byte) Integer.parseInt(value.substring(s, e), 2);
            value = value.substring(0, s);
        }
        return bytes;
    }

    private static Map<Integer, DefaultValueMapper> createDefaultValueMappers(YashanDBConnection jdbcConnection) {
        // Data types that are supported should be registered in the map.
        final Map<Integer, DefaultValueMapper> result = new HashMap<>();

        // Numeric types
        result.put(YasTypes.NUMERIC, nullableDefaultValueMapper());
        result.put(YasTypes.DECIMAL, nullableDefaultValueMapper());
        result.put(YasTypes.INTEGER, nullableDefaultValueMapper());

        // Approximate numerics
        result.put(YasTypes.BINARY_FLOAT, nullableDefaultValueMapper());
        result.put(YasTypes.FLOAT, nullableDefaultValueMapper());
        result.put(YasTypes.BIGINT, nullableDefaultValueMapper());
        result.put(YasTypes.SMALLINT, nullableDefaultValueMapper());
        result.put(YasTypes.TINYINT, nullableDefaultValueMapper());
        result.put(YasTypes.BOOLEAN, nullableDefaultValueMapper(convertBoolean()));
        result.put(YasTypes.REAL, nullableDefaultValueMapper());
        result.put(YasTypes.DOUBLE, nullableDefaultValueMapper());

        // Date and time
        result.put(YasTypes.DATE, nullableDefaultValueMapper(convertDate(jdbcConnection)));
        result.put(YasTypes.TIME, nullableDefaultValueMapper(convertTime(jdbcConnection)));
        result.put(YasTypes.TIMESTAMP, nullableDefaultValueMapper(convertTimestamp(jdbcConnection)));
        result.put(YasTypes.TIMESTAMP_TZ, nullableDefaultValueMapper(convertTimestamp(jdbcConnection)));
        result.put(YasTypes.YM_INTERVAL, nullableDefaultValueMapper(convertIntervalYearMonthStringLiteral()));
        result.put(YasTypes.DS_INTERVAL, nullableDefaultValueMapper(convertIntervalDaySecondStringLiteral()));

        // Character strings
        result.put(YasTypes.CHAR, nullableDefaultValueMapper(enforceCharFieldPadding()));
        result.put(YasTypes.VARCHAR, nullableDefaultValueMapper(enforceStringUnquote()));

        // Unicode character strings
        result.put(YasTypes.NCHAR, nullableDefaultValueMapper(enforceCharFieldPadding()));
        result.put(YasTypes.NVARCHAR, nullableDefaultValueMapper(enforceStringUnquote()));

        // Other data types have been omitted.
        return result;
    }

    private static DefaultValueMapper nullableDefaultValueMapper() {
        return nullableDefaultValueMapper(null);
    }

    private static DefaultValueMapper nullableDefaultValueMapper(DefaultValueMapper mapper) {
        return (column, value) -> {
            if ("NULL".equalsIgnoreCase(value)) {
                return null;
            }
            if (mapper != null) {
                return mapper.parse(column, value);
            }
            return value;
        };
    }

    private static DefaultValueMapper convertBoolean() {
        return (column, value) -> {
            try {
                return Integer.parseInt(value) != 0;
            }
            catch (NumberFormatException ignore) {
                return Boolean.parseBoolean(value);
            }
        };
    }

    private static DefaultValueMapper convertIntervalDaySecondStringLiteral() {
        return (column, value) -> {
            return value;
        };
    }

    private static DefaultValueMapper convertIntervalYearMonthStringLiteral() {
        return (column, value) -> {
            return value;
        };
    }

    private static DefaultValueMapper enforceCharFieldPadding() {
        return (column, value) -> value != null ? Strings.pad(unquote(value), column.length(), ' ') : null;
    }

    private static DefaultValueMapper enforceStringUnquote() {
        return (column, value) -> value != null ? unquote(value) : null;
    }

    private static DefaultValueMapper convertDate(YashanDBConnection jdbcConnection) {
        return (column, value) -> {
            if ("SYSDATE".equalsIgnoreCase(value.trim()) || value.trim().toUpperCase().equalsIgnoreCase("CURRENT_TIMESTAMP")) {
                if (column.isOptional()) {
                    // If the column is optional, the default value is ignored
                    return null;
                }
                else if (column.jdbcType() == YasTypes.TIMESTAMP_TZ) {
                    // If the column is a TIMESTAMP WITH [LOCAL] TIME ZONE, the non-null default is based on EPOCH
                    return Date.from(Instant.EPOCH);
                }
                else if (column.jdbcType() == YasTypes.DATE || column.jdbcType() == YasTypes.TIMESTAMP) {
                    // If the column is a TIMESTAMP WITH [LOCAL] TIME ZONE, the non-null default is based on EPOCH
                    return Date.from(Instant.EPOCH);
                }
                else if (column.jdbcType() == YasTypes.TIME) {
                    // If the column is a TIMESTAMP WITH [LOCAL] TIME ZONE, the non-null default is based on EPOCH
                    return Time.from(Instant.EPOCH);
                }
                else {
                    // For all other temporal types, return "0".
                    // The return is a string-value as the YashanDBValueConverters know how to explicitly infer
                    // whether to emit the final converted value as either a string or numeric value based on
                    // the column's data type.
                    return "0";
                }
            }

            String defaultValue;
            if (value.startsWith("'")) {
                defaultValue = value.substring(1, value.length() - 1);
            }
            else {
                defaultValue = value;
            }
            if (DATE_PATTERN.matcher(defaultValue).matches()) {
                return java.sql.Date.valueOf(LocalDate.parse(defaultValue));
            }
            else if (DATE_PATTERN.matcher(defaultValue).matches()) {
                return Date.from(LocalDateTime.parse(TIMESTAMP_PATTERN.matcher(defaultValue).group(0)).atZone(ZoneId.systemDefault()).toInstant());
            }

            return value;
        };
    }

    private static DefaultValueMapper convertTime(YashanDBConnection jdbcConnection) {
        return (column, value) -> {
            if ("SYSDATE".equalsIgnoreCase(value.trim()) || value.trim().toUpperCase().equalsIgnoreCase("CURRENT_TIMESTAMP")) {
                if (column.isOptional()) {
                    // If the column is optional, the default value is ignored
                    return null;
                }
                else if (column.jdbcType() == YasTypes.TIME) {
                    // If the column is a TIMESTAMP WITH [LOCAL] TIME ZONE, the non-null default is based on EPOCH
                    return Time.from(Instant.EPOCH);
                }
                else {
                    // For all other temporal types, return "0".
                    // The return is a string-value as the YashanDBValueConverters know how to explicitly infer
                    // whether to emit the final converted value as either a string or numeric value based on
                    // the column's data type.
                    return "0";
                }
            }

            String defaultValue;
            if (value.startsWith("'")) {
                defaultValue = value.substring(1, value.length() - 1);
            }
            else {
                defaultValue = value;
            }

            return convertToDuration(column, defaultValue);
        };
    }

    private static Object convertToDuration(Column column, String value) {
        String data = "";
        if (value != null) {
            String trim = value.trim();
            if (value.startsWith("'")) {
                data = trim.substring(1, trim.length() - 1);
            }
            else {
                data = trim;
            }
        }
        Matcher matcher = TIMESTAMP_PATTERN.matcher(data);
        if (matcher.matches()) {
            data = matcher.group(2);
        }
        return stringToDuration(data);
    }

    public static Duration stringToDuration(String timeString) {
        final Matcher matcher = TIME_FIELD_PATTERN.matcher(timeString);
        if (!matcher.matches()) {
            throw new DebeziumException("Unexpected format for TIME column: " + timeString);
        }

        final boolean isNegative = !timeString.isBlank() && timeString.charAt(0) == '-';
        final long hours = Long.parseLong(matcher.group(1));
        final long minutes = Long.parseLong(matcher.group(2));
        final String secondsGroup = matcher.group(4);

        long seconds = 0;
        long nanoSeconds = 0;

        if (!Objects.isNull(secondsGroup)) {
            seconds = Long.parseLong(secondsGroup);
            String microSecondsString = matcher.group(6);
            if (!Objects.isNull(microSecondsString)) {
                nanoSeconds = Long.parseLong(Strings.justifyLeft(microSecondsString, 9, '0'));
            }
        }

        final Duration duration = hours >= 0
                ? Duration
                        .ofHours(hours)
                        .plusMinutes(minutes)
                        .plusSeconds(seconds)
                        .plusNanos(nanoSeconds)
                : Duration
                        .ofHours(hours)
                        .minusMinutes(minutes)
                        .minusSeconds(seconds)
                        .minusNanos(nanoSeconds);
        return isNegative && !duration.isNegative() ? duration.negated() : duration;
    }

    private static DefaultValueMapper convertTimestamp(YashanDBConnection jdbcConnection) {
        return (column, value) -> {
            if ("SYSDATE".equalsIgnoreCase(value.trim()) || value.trim().toUpperCase().equalsIgnoreCase("CURRENT_TIMESTAMP")) {
                if (column.isOptional()) {
                    // If the column is optional, the default value is ignored
                    return null;
                }
                if (column.jdbcType() == YasTypes.TIMESTAMP) {
                    // If the column is a TIMESTAMP WITH [LOCAL] TIME ZONE, the non-null default is based on EPOCH
                    return Date.from(Instant.EPOCH);
                }
                else if (column.jdbcType() == YasTypes.TIME) {
                    // If the column is a TIMESTAMP WITH [LOCAL] TIME ZONE, the non-null default is based on EPOCH
                    return Time.from(Instant.EPOCH);
                }
                else {
                    // For all other temporal types, return "0".
                    // The return is a string-value as the YashanDBValueConverters know how to explicitly infer
                    // whether to emit the final converted value as either a string or numeric value based on
                    // the column's data type.
                    return "0";
                }
            }

            String defaultValue;
            if (value.startsWith("'")) {
                defaultValue = value.substring(1, value.length() - 1);
            }
            else {
                defaultValue = value;
            }
            if (DATE_PATTERN.matcher(defaultValue).matches()) {
                return java.sql.Date.valueOf(LocalDate.parse(defaultValue));
            }
            else if (DATE_PATTERN.matcher(defaultValue).matches()) {
                return Date.from(LocalDateTime.parse(TIMESTAMP_PATTERN.matcher(defaultValue).group(0)).atZone(ZoneId.systemDefault()).toInstant());
            }

            return value;
        };
    }

    private static DefaultValueMapper castTemporalFunctionCall(YashanDBConnection jdbcConnection) {
        return (column, value) -> {
            if ("SYSDATE".equalsIgnoreCase(value.trim()) || value.trim().toUpperCase().equalsIgnoreCase("CURRENT_TIMESTAMP")) {
                if (column.isOptional()) {
                    // If the column is optional, the default value is ignored
                    return null;
                }
                else if (column.jdbcType() == YasTypes.TIMESTAMP_TZ) {
                    // If the column is a TIMESTAMP WITH [LOCAL] TIME ZONE, the non-null default is based on EPOCH
                    return Date.from(Instant.EPOCH);
                }
                else if (column.jdbcType() == YasTypes.DATE || column.jdbcType() == YasTypes.TIMESTAMP) {
                    // If the column is a TIMESTAMP WITH [LOCAL] TIME ZONE, the non-null default is based on EPOCH
                    return Date.from(Instant.EPOCH);
                }
                else if (column.jdbcType() == YasTypes.TIME) {
                    // If the column is a TIMESTAMP WITH [LOCAL] TIME ZONE, the non-null default is based on EPOCH
                    return Time.from(Instant.EPOCH);
                }
                else {
                    // For all other temporal types, return "0".
                    // The return is a string-value as the YashanDBValueConverters know how to explicitly infer
                    // whether to emit the final converted value as either a string or numeric value based on
                    // the column's data type.
                    return "0";
                }
            }
            return value;
        };
    }

    private static String unquote(String value) {
        if (value.startsWith("('") && value.endsWith("')")) {
            return value.substring(2, value.length() - 2);
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
