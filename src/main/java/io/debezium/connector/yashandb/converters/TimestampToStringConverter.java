/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.converters;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.debezium.function.Predicates;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.util.Strings;

/**
 * YashanDB reports {@code Timestamp} as a long date type by default.  There may be some cases
 * where the consumer would prefer this to be translated to a {@code String} and this converter
 * provides this behavior out of the box.
 */
public class TimestampToStringConverter implements CustomConverter<SchemaBuilder, RelationalColumn>, ConfigDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimestampToStringConverter.class);

    public static final String SELECTOR_PROPERTY = "selector";

    private Predicate<RelationalColumn> selector = x -> true;
    private DateTimeFormatter formatter; // 存储配置参数

    @Override
    public void configure(Properties props) {
        String datetimeFormat = props.getProperty("format", "yyyy-MM-dd HH:mm:ss.SSSSSS");
        formatter = DateTimeFormatter.ofPattern(datetimeFormat);
        final String selectorConfig = props.getProperty(SELECTOR_PROPERTY);
        if (Strings.isNullOrEmpty(selectorConfig)) {
            return;
        }
        selector = Predicates.includes(selectorConfig.trim(), x -> x.dataCollection() + "." + x.name());
    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        // 兼容yashandb23.4.7.1版本之后的TIMESTAMP(0)~TIMESTAMP(6)类型
        final String typeName = field.typeName();
        final boolean isPlainTimestamp = typeName != null
                && typeName.trim().matches("(?i)^TIMESTAMP(\\s*\\(\\s*(?:[0-6])\\s*\\))?$");
        if (!isPlainTimestamp || !selector.test(field)) {
            return;
        }
        registration.register(SchemaBuilder.string(), x -> {
            if (x == null) {
                if (field.isOptional()) {
                    return null;
                }
                else if (field.hasDefaultValue()) {
                    return field.defaultValue();
                }
                else {
                    return null;
                }
            }
            if (x instanceof Long) {
                // 假设为微秒级时间戳
                long epochMicros = (Long) x;
                long epochMillis = epochMicros / 1000L;
                return formatter.format(Instant.ofEpochMilli(epochMillis).atZone(ZoneId.systemDefault()));
            }
            else if (x instanceof String) {
                return x;
            }
            else if (x instanceof Timestamp) {
                return formatter.format(((Timestamp) x).toLocalDateTime());
            }
            LOGGER.warn("Cannot convert '{}' to string", x.getClass());
            return x.toString();
        });
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(Field.create(SELECTOR_PROPERTY), Field.create("format"));
    }
}
