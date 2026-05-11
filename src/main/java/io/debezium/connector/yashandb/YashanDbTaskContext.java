/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * Provides context for the YashanDB connector task, including configuration and custom metric tags.
 *
 * @author Debezium Authors
 */
public class YashanDbTaskContext extends CdcSourceTaskContext<YashanDbConnectorConfig> {

    /**
     * Creates a YashanDbTaskContext instance with the given configuration.
     *
     * @param rawConfig the raw configuration
     * @param config the connector configuration
     */
    public YashanDbTaskContext(Configuration rawConfig, YashanDbConnectorConfig config) {
        super(rawConfig, config, config.getCustomMetricTags());
    }
}
