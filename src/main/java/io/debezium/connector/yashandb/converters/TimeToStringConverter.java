/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.converters;

import java.sql.Time;
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
 * YashanDB reports {@code Time} as a long data type by default.  There may be some cases
 * where the consumer would prefer this to be translated to a {@code String} and this converter
 * provides this behavior out of the box.
 */
public class TimeToStringConverter implements CustomConverter<SchemaBuilder, RelationalColumn>, ConfigDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeToStringConverter.class);

    public static final String SELECTOR_PROPERTY = "selector";

    private Predicate<RelationalColumn> selector = x -> true;
    private DateTimeFormatter formatter;

    /**
     * Configures the time-to-string converter with datetime format and column selector.
     *
     * @param props configuration properties containing optional {@code format} and {@code selector} keys
     */
    @Override
    public void configure(Properties props) {
        String datetimeFormat = props.getProperty("format", "HH:mm:ss.SSSSSS");
        formatter = DateTimeFormatter.ofPattern(datetimeFormat);
        final String selectorConfig = props.getProperty(SELECTOR_PROPERTY);
        if (Strings.isNullOrEmpty(selectorConfig)) {
            return;
        }
        selector = Predicates.includes(selectorConfig.trim(), x -> x.dataCollection() + "." + x.name());
    }

    /**
     * Registers a string converter for TIME columns that match the configured selector.
     *
     * @param field the relational column to check for conversion
     * @param registration the registration callback to register the converter
     */
    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        if (!"TIME".equalsIgnoreCase(field.typeName()) || !selector.test(field)) {
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
            if (x instanceof Time) {
                return formatter.format(((Time) x).toLocalTime());
            }
            else if (x instanceof Long) {
                // Assumed to be a microsecond-precision epoch timestamp
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

    /**
     * Returns the configuration fields supported by this converter.
     *
     * @return the set of configuration fields
     */
    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(Field.create(SELECTOR_PROPERTY), Field.create("format"));
    }
}
