/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.converters;

import io.debezium.function.Predicates;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.util.Strings;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.function.Predicate;

/**
 * YashanDB reports {@code Timestamp} as a long column type by default.  There may be some cases
 * where the consumer would prefer this to be translated to a {@code Timestamp} string format.
 */
public class DateToStringConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DateToStringConverter.class);

    public static final String SELECTOR_PROPERTY = "selector";

    private Predicate<RelationalColumn> selector = x -> true;
    private DateTimeFormatter formatter;    // 存储配置参数

    @Override
    public void configure(Properties props) {
        String datetimeFormat = props.getProperty("format", "yyyy-MM-dd");
        formatter = DateTimeFormatter.ofPattern(datetimeFormat);
        final String selectorConfig = props.getProperty(SELECTOR_PROPERTY);
        if (Strings.isNullOrEmpty(selectorConfig)) {
            return;
        }
        selector = Predicates.includes(selectorConfig.trim(), x -> x.dataCollection() + "." + x.name());
    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        if (!"DATE".equalsIgnoreCase(field.typeName()) || !selector.test(field)) {
            return;
        }
        registration.register(SchemaBuilder.string(), x -> {
            if (x == null) {
                if (field.isOptional()) {
                    return null;
                } else if (field.hasDefaultValue()) {
                    return field.defaultValue();
                } else {
                    return null;
                }
            }
            if (x instanceof Long) {
                // 假设为微秒级时间戳
                long epochMicros = (Long) x;
                long epochMillis = epochMicros / 1000L;
                return formatter.format(Instant.ofEpochMilli(epochMillis).atZone(ZoneId.systemDefault()));
            } else if (x instanceof String) {
                return x;
            } else if (x instanceof Timestamp) {
                return formatter.format(((Timestamp) x).toLocalDateTime());
            } else if (x instanceof Date) {
                return formatter.format(Instant.ofEpochMilli(((Date) x).getTime()).atZone(ZoneId.systemDefault()));
            } else if (x instanceof java.util.Date) {
                return formatter.format(((java.util.Date) x).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
            }
            LOGGER.warn("Cannot convert '{}' to string", x.getClass());
            return x.toString();
        });
    }
}
