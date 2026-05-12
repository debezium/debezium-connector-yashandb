/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;

public class YashanDbTaskContext extends CdcSourceTaskContext<YashanDbConnectorConfig> {

    public YashanDbTaskContext(Configuration rawConfig, YashanDbConnectorConfig config) {
        super(rawConfig, config, config.getCustomMetricTags());
    }
}
