/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.outbox;

import java.util.Map;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.yashandb.Module;
import io.debezium.transforms.outbox.EventRouterConfigDefinition;
import io.debezium.transforms.outbox.EventRouterConfigurationProvider;

/**
 * An implementation of the {@link EventRouterConfigurationProvider} for the YashanDB connector.
 */
public class YashanDbEventRouterConfigurationProvider implements EventRouterConfigurationProvider {

    private Configuration configuration;

    /**
     * Returns the name of this configuration provider.
     *
     * @return the provider name
     */
    @Override
    public String getName() {
        return Module.name();
    }

    /**
     * Configures this provider from the given configuration map.
     *
     * @param configMap the configuration key-value pairs
     */
    @Override
    public void configure(Map<String, ?> configMap) {
        this.configuration = Configuration.from(configMap);
    }

    /**
     * Returns the configured event ID field name in uppercase.
     *
     * @return the field name for event ID
     */
    @Override
    public String getFieldEventId() {
        return getStringWithUpperCaseDefault(EventRouterConfigDefinition.FIELD_EVENT_ID);
    }

    /**
     * Returns the configured event key field name in uppercase.
     *
     * @return the field name for event key
     */
    @Override
    public String getFieldEventKey() {
        return getStringWithUpperCaseDefault(EventRouterConfigDefinition.FIELD_EVENT_KEY);
    }

    /**
     * Returns the configured event timestamp field name in uppercase.
     *
     * @return the field name for event timestamp
     */
    @Override
    public String getFieldEventTimestamp() {
        return getStringWithUpperCaseDefault(EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP);
    }

    /**
     * Returns the configured payload field name in uppercase.
     *
     * @return the field name for payload
     */
    @Override
    public String getFieldPayload() {
        return getStringWithUpperCaseDefault(EventRouterConfigDefinition.FIELD_PAYLOAD);
    }

    /**
     * Returns the configured route-by field name in uppercase.
     *
     * @return the field name used to determine the event route
     */
    @Override
    public String getRouteByField() {
        return getStringWithUpperCaseDefault(EventRouterConfigDefinition.ROUTE_BY_FIELD);
    }

    private String getStringWithUpperCaseDefault(Field field) {
        if (configuration == null) {
            throw new DebeziumException("Event router configuration for YashanDB has not yet been configured");
        }

        // Check if the configuration option is defined by the user and if so; use it.
        if (configuration.hasKey(field.name())) {
            return configuration.getString(field);
        }

        // Configuration option isn't defined by the user, use connector-specific fallback value.
        if (field.defaultValue() != null) {
            return field.defaultValueAsString().toUpperCase();
        }

        // no default value was supplied on the field
        return null;
    }
}
