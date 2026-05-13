/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Metrics factory for YashanDB change event sources.
 */
public class YashanDbChangeEventSourceMetricsFactory extends DefaultChangeEventSourceMetricsFactory<YashanDbPartition> {

    private final YashanDbStreamingChangeEventSourceMetrics streamingMetrics;

    /**
     * Creates a new metrics factory.
     *
     * @param streamingMetrics the streaming metrics instance
     */
    public YashanDbChangeEventSourceMetricsFactory(YashanDbStreamingChangeEventSourceMetrics streamingMetrics) {
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<YashanDbPartition> getStreamingMetrics(T taskContext,
                                                                                                                     ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                     EventMetadataProvider eventMetadataProvider,
                                                                                                                     CapturedTablesSupplier capturedTablesSupplier) {
        return streamingMetrics;
    }
}
