/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sics.ystream.result.Position;

import io.debezium.connector.yashandb.YashanDbOffsetContext;
import io.debezium.connector.yashandb.YashanDbPartition;
import io.debezium.connector.yashandb.YashanDbStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset LCR position is compared against the value captured when the monitor was last
 * consulted, and when the position has not moved, a warning is logged and the streaming
 * metrics warning count is incremented.
 *
 * @author Chris Cranford
 */
public class YStreamOffsetActivityMonitor implements OffsetActivityMonitor<YashanDbPartition, YashanDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YStreamOffsetActivityMonitor.class);

    private final Duration checkInterval;
    private final YashanDbStreamingChangeEventSourceMetrics metrics;

    private Position previousLcrPosition;

    public YStreamOffsetActivityMonitor(Duration checkInterval, YashanDbStreamingChangeEventSourceMetrics metrics) {
        this.checkInterval = checkInterval;
        this.metrics = metrics;
    }

    @Override
    public void checkForStaleOffsets(YashanDbPartition partition, YashanDbOffsetContext offsetContext) {
        final Position lcrPosition = offsetContext.getLcrPosition();

        // Check for stale state
        if (Objects.equals(previousLcrPosition, lcrPosition)) {
            LOGGER.warn("Offset LCR position {} has not changed in {} milliseconds. " +
                    "This may indicate the database is idle, there are no changes for the captured tables, " +
                    "or that there are long running transaction(s) delaying the delivery of change events.",
                    previousLcrPosition, checkInterval.toMillis());
            metrics.incrementWarningCount();
        }

        // Update tracked stats
        previousLcrPosition = lcrPosition;
    }

}
