/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import java.time.Duration;
import java.util.Objects;

import com.sics.ystream.result.Position;

import io.debezium.connector.yashandb.YashanDbOffsetContext;
import io.debezium.connector.yashandb.YashanDbPartition;
import io.debezium.connector.yashandb.YashanDbStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.StaleOffsetsResult;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset LCR position is compared against the value captured when the monitor was last
 * consulted, and when the position has not moved, a stale result is reported and the streaming
 * metrics warning count is incremented.
 *
 * @author Chris Cranford
 */
public class YStreamOffsetActivityMonitor implements OffsetActivityMonitor<YashanDbPartition, YashanDbOffsetContext> {

    private final Duration checkInterval;
    private final YashanDbStreamingChangeEventSourceMetrics metrics;

    private Position previousLcrPosition;

    public YStreamOffsetActivityMonitor(Duration checkInterval, YashanDbStreamingChangeEventSourceMetrics metrics) {
        this.checkInterval = checkInterval;
        this.metrics = metrics;
    }

    @Override
    public StaleOffsetsResult checkForStaleOffsets(YashanDbPartition partition, YashanDbOffsetContext offsetContext) {
        final Position lcrPosition = offsetContext.getLcrPosition();

        // Check for stale state
        StaleOffsetsResult result = StaleOffsetsResult.fresh();
        if (Objects.equals(previousLcrPosition, lcrPosition)) {
            result = StaleOffsetsResult.stale(
                    ("Offset LCR position %s has not changed in %d milliseconds. " +
                            "This may indicate the database is idle, there are no changes for the captured tables, " +
                            "or that there are long running transaction(s) delaying the delivery of change events.")
                            .formatted(previousLcrPosition, checkInterval.toMillis()));
            metrics.incrementWarningCount();
        }

        // Update tracked stats
        previousLcrPosition = lcrPosition;

        return result;
    }

}
