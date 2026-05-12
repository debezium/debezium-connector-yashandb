/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * The JMX exposed interface for YashanDB streaming metrics.
 */
public interface YashanDbStreamingChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {

    /**
     * @return number of warnings detected by the connector
     */
    long getWarningCount();

    /**
     * @return number of errors detected by the connector
     */
    long getErrorCount();
}
