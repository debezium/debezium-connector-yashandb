/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import io.debezium.connector.common.CdcSourceTaskContext;

public class YashanDBTaskContext extends CdcSourceTaskContext<YashanDBOffsetContext> {

    public YashanDBTaskContext(YashanDBConnectorConfig config, YashanDBDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), config.getCustomMetricTags(), schema::tableIds);
    }
}
