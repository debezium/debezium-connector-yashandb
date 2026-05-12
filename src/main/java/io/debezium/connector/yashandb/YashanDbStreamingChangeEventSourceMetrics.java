/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.util.LRUCacheMap;

/**
 * The metrics implementation for YashanDB connector streaming phase.
 */
@ThreadSafe
/**
 * YashanDB streaming metrics.
 */
public class YashanDbStreamingChangeEventSourceMetrics extends DefaultStreamingChangeEventSourceMetrics<YashanDbPartition>
        implements YashanDbStreamingChangeEventSourceMetricsMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDbStreamingChangeEventSourceMetrics.class);

    private final AtomicInteger totalCapturedDmlCount = new AtomicInteger();
    private final AtomicInteger lastCapturedDmlCount = new AtomicInteger();
    private final AtomicLong maxCapturedDmlCount = new AtomicLong();
    private final AtomicLong totalProcessedRows = new AtomicLong();
    private final AtomicReference<Duration> totalParseTime = new AtomicReference<>();

    private final AtomicInteger batchSize = new AtomicInteger();

    private final AtomicLong networkConnectionProblemsCounter = new AtomicLong();
    private final AtomicLong registeredDmlCount = new AtomicLong();
    private final AtomicLong committedTransactions = new AtomicLong();

    private final AtomicLong committedDmlCount = new AtomicLong();
    private final AtomicInteger errorCount = new AtomicInteger();
    private final AtomicInteger warningCount = new AtomicInteger();
    private final AtomicLong timeDifference = new AtomicLong();
    private final AtomicReference<ZoneOffset> zoneOffset = new AtomicReference<>();
    private final AtomicReference<Scn> offsetScn = new AtomicReference<>();
    private final AtomicInteger unparsableDdlCount = new AtomicInteger();

    /**
     * Creates a new streaming change event source metrics instance.
     *
     * @param taskContext the task context
     * @param changeEventQueueMetrics the change event queue metrics
     * @param metadataProvider the event metadata provider
     * @param connectorConfig the connector configuration
     * @param capturedTablesSupplier the captured tables supplier
     */
    public YashanDbStreamingChangeEventSourceMetrics(CdcSourceTaskContext taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                     EventMetadataProvider metadataProvider,
                                                     YashanDbConnectorConfig connectorConfig,
                                                     CapturedTablesSupplier capturedTablesSupplier) {
        this(taskContext, changeEventQueueMetrics, metadataProvider, connectorConfig, Clock.systemUTC(), capturedTablesSupplier);
    }

    /**
     * Constructor that allows providing a clock to be used for Tests.
     */
    @VisibleForTesting
    YashanDbStreamingChangeEventSourceMetrics(CdcSourceTaskContext taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                              EventMetadataProvider metadataProvider,
                                              YashanDbConnectorConfig connectorConfig,
                                              Clock clock,
                                              CapturedTablesSupplier capturedTablesSupplier) {
        super(taskContext, changeEventQueueMetrics, metadataProvider, capturedTablesSupplier);
        timeDifference.set(0L);
        zoneOffset.set(ZoneOffset.UTC);
        offsetScn.set(Scn.NULL);
        reset();
    }

    /** {@inheritDoc} */
    @Override
    public void reset() {
        totalCapturedDmlCount.set(0);
        totalProcessedRows.set(0);
        lastCapturedDmlCount.set(0);
        maxCapturedDmlCount.set(0);
        networkConnectionProblemsCounter.set(0);
        totalParseTime.set(Duration.ZERO);
        registeredDmlCount.set(0);
        committedDmlCount.set(0);
        errorCount.set(0);
        warningCount.set(0);
    }

    /** {@inheritDoc} */
    @Override
    public long getNumberOfCommittedTransactions() {
        return committedTransactions.get();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long getErrorCount() {
        return errorCount.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getWarningCount() {
        return warningCount.get();
    }

    /**
     * Increments the warning count.
     */
    public void incrementWarningCount() {
        warningCount.incrementAndGet();
    }

    /**
     * Increments the unparsable DDL count.
     */
    public void incrementUnparsableDdlCount() {
        unparsableDdlCount.incrementAndGet();
    }


    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "YashanDbStreamingChangeEventSourceMetrics{" +
                ", offsetScn=" + offsetScn.get() +
                ", totalProcessedRows=" + totalProcessedRows +
                ", totalCapturedDmlCount=" + totalCapturedDmlCount +
                ", lastCapturedDmlCount=" + lastCapturedDmlCount +
                ", maxCapturedDmlCount=" + maxCapturedDmlCount +
                ", batchSize=" + batchSize +
                ", networkConnectionProblemsCounter" + networkConnectionProblemsCounter +
                ", totalParseTime=" + totalParseTime +
                ", committedTransactions=" + committedTransactions.get() +
                ", registeredDmlCount=" + registeredDmlCount.get() +
                ", committedDmlCount=" + committedDmlCount.get() +
                ", errorCount=" + errorCount.get() +
                ", warningCount=" + warningCount.get() +
                ", unparsableDdlCount=" + unparsableDdlCount.get() +
                '}';
    }
}
