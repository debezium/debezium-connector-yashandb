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
import java.util.Map;
import java.util.Set;
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

    private static final long MILLIS_PER_SECOND = 1000L;
    private static final int TRANSACTION_ID_SET_SIZE = 10;

    private final AtomicReference<Scn> currentScn = new AtomicReference<>();
    private final AtomicInteger logMinerQueryCount = new AtomicInteger();
    private final AtomicInteger totalCapturedDmlCount = new AtomicInteger();
    private final AtomicReference<Duration> totalDurationOfFetchingQuery = new AtomicReference<>();
    private final AtomicInteger lastCapturedDmlCount = new AtomicInteger();
    private final AtomicReference<Duration> lastDurationOfFetchingQuery = new AtomicReference<>();
    private final AtomicLong maxCapturedDmlCount = new AtomicLong();
    private final AtomicLong totalProcessedRows = new AtomicLong();
    private final AtomicReference<Duration> maxDurationOfFetchingQuery = new AtomicReference<>();
    private final AtomicReference<Duration> totalBatchProcessingDuration = new AtomicReference<>();
    private final AtomicReference<Duration> lastBatchProcessingDuration = new AtomicReference<>();
    private final AtomicReference<Duration> totalParseTime = new AtomicReference<>();
    private final AtomicReference<Duration> totalStartLogMiningSessionDuration = new AtomicReference<>();
    private final AtomicReference<Duration> lastStartLogMiningSessionDuration = new AtomicReference<>();
    private final AtomicReference<Duration> maxStartingLogMiningSessionDuration = new AtomicReference<>();
    private final AtomicReference<Duration> totalProcessingTime = new AtomicReference<>();
    private final AtomicReference<Duration> minBatchProcessingTime = new AtomicReference<>();
    private final AtomicReference<Duration> maxBatchProcessingTime = new AtomicReference<>();
    private final AtomicReference<Duration> totalResultSetNextTime = new AtomicReference<>();
    private final AtomicLong maxBatchProcessingThroughput = new AtomicLong();
    private final AtomicReference<String[]> currentLogFileName;
    private final AtomicReference<String[]> redoLogStatus;
    private final AtomicLong minimumLogsMined = new AtomicLong();
    private final AtomicLong maximumLogsMined = new AtomicLong();
    private final AtomicInteger switchCounter = new AtomicInteger();

    private final AtomicInteger batchSize = new AtomicInteger();
    private final AtomicLong millisecondToSleepBetweenMiningQuery = new AtomicLong();

    private final AtomicLong networkConnectionProblemsCounter = new AtomicLong();

    private final AtomicReference<Duration> keepTransactionsDuration = new AtomicReference<>();
    private final AtomicReference<Duration> lagFromTheSourceDuration = new AtomicReference<>();
    private final AtomicReference<Duration> minLagFromTheSourceDuration = new AtomicReference<>();
    private final AtomicReference<Duration> maxLagFromTheSourceDuration = new AtomicReference<>();
    private final AtomicReference<Duration> lastCommitDuration = new AtomicReference<>();
    private final AtomicReference<Duration> maxCommitDuration = new AtomicReference<>();
    private final AtomicLong activeTransactions = new AtomicLong();
    private final AtomicLong rolledBackTransactions = new AtomicLong();
    private final AtomicLong committedTransactions = new AtomicLong();
    private final AtomicLong oversizedTransactions = new AtomicLong();
    private final AtomicReference<LRUCacheMap<String, String>> abandonedTransactionIds = new AtomicReference<>();
    private final AtomicReference<LRUCacheMap<String, String>> rolledBackTransactionIds = new AtomicReference<>();
    private final AtomicLong registeredDmlCount = new AtomicLong();
    private final AtomicLong committedDmlCount = new AtomicLong();
    private final AtomicInteger errorCount = new AtomicInteger();
    private final AtomicInteger warningCount = new AtomicInteger();
    private final AtomicInteger scnFreezeCount = new AtomicInteger();
    private final AtomicLong timeDifference = new AtomicLong();
    private final AtomicReference<ZoneOffset> zoneOffset = new AtomicReference<>();
    private final AtomicReference<Scn> oldestScn = new AtomicReference<>();
    private final AtomicReference<Scn> committedScn = new AtomicReference<>();
    private final AtomicReference<Scn> offsetScn = new AtomicReference<>();
    private final AtomicInteger unparsableDdlCount = new AtomicInteger();
    private final AtomicLong miningSessionUserGlobalAreaMemory = new AtomicLong();
    private final AtomicLong miningSessionUserGlobalAreaMaxMemory = new AtomicLong();
    private final AtomicLong miningSessionProcessGlobalAreaMemory = new AtomicLong();
    private final AtomicLong miningSessionProcessGlobalAreaMaxMemory = new AtomicLong();

    // Constants for sliding window algorithm
    private final int batchSizeMin;
    private final int batchSizeMax;
    private final int batchSizeDefault;

    // constants for sleeping algorithm
    private final long sleepTimeMin;
    private final long sleepTimeMax;
    private final long sleepTimeDefault;
    private final long sleepTimeIncrement;

    private final Instant startTime;

    private final Clock clock;

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

        this.clock = clock;
        startTime = clock.instant();
        timeDifference.set(0L);
        zoneOffset.set(ZoneOffset.UTC);

        currentScn.set(Scn.NULL);
        oldestScn.set(Scn.NULL);
        offsetScn.set(Scn.NULL);
        committedScn.set(Scn.NULL);

        currentLogFileName = new AtomicReference<>(new String[0]);
        minimumLogsMined.set(0L);
        maximumLogsMined.set(0L);
        redoLogStatus = new AtomicReference<>(new String[0]);
        switchCounter.set(0);

        batchSizeDefault = connectorConfig.getLogMiningBatchSizeDefault();
        batchSizeMin = connectorConfig.getLogMiningBatchSizeMin();
        batchSizeMax = connectorConfig.getLogMiningBatchSizeMax();

        sleepTimeDefault = connectorConfig.getLogMiningSleepTimeDefault().toMillis();
        sleepTimeMin = connectorConfig.getLogMiningSleepTimeMin().toMillis();
        sleepTimeMax = connectorConfig.getLogMiningSleepTimeMax().toMillis();
        sleepTimeIncrement = connectorConfig.getLogMiningSleepTimeIncrement().toMillis();

        keepTransactionsDuration.set(connectorConfig.getLogMiningTransactionRetention());

        reset();
    }

    @Override
    /** {@inheritDoc} */
    public void reset() {
        batchSize.set(batchSizeDefault);
        millisecondToSleepBetweenMiningQuery.set(sleepTimeDefault);
        totalCapturedDmlCount.set(0);
        totalProcessedRows.set(0);
        maxDurationOfFetchingQuery.set(Duration.ZERO);
        lastDurationOfFetchingQuery.set(Duration.ZERO);
        logMinerQueryCount.set(0);
        totalDurationOfFetchingQuery.set(Duration.ZERO);
        lastCapturedDmlCount.set(0);
        maxCapturedDmlCount.set(0);
        totalBatchProcessingDuration.set(Duration.ZERO);
        maxBatchProcessingThroughput.set(0);
        lastBatchProcessingDuration.set(Duration.ZERO);
        networkConnectionProblemsCounter.set(0);
        totalParseTime.set(Duration.ZERO);
        totalStartLogMiningSessionDuration.set(Duration.ZERO);
        lastStartLogMiningSessionDuration.set(Duration.ZERO);
        maxStartingLogMiningSessionDuration.set(Duration.ZERO);
        totalProcessingTime.set(Duration.ZERO);
        minBatchProcessingTime.set(Duration.ZERO);
        maxBatchProcessingTime.set(Duration.ZERO);
        totalResultSetNextTime.set(Duration.ZERO);
        miningSessionUserGlobalAreaMemory.set(0L);
        miningSessionUserGlobalAreaMaxMemory.set(0L);
        miningSessionProcessGlobalAreaMemory.set(0L);
        miningSessionProcessGlobalAreaMaxMemory.set(0L);

        // transactional buffer metrics
        lagFromTheSourceDuration.set(Duration.ZERO);
        maxLagFromTheSourceDuration.set(Duration.ZERO);
        minLagFromTheSourceDuration.set(Duration.ZERO);
        lastCommitDuration.set(Duration.ZERO);
        maxCommitDuration.set(Duration.ZERO);
        activeTransactions.set(0);
        rolledBackTransactions.set(0);
        committedTransactions.set(0);
        oversizedTransactions.set(0);
        registeredDmlCount.set(0);
        committedDmlCount.set(0);
        abandonedTransactionIds.set(new LRUCacheMap<>(TRANSACTION_ID_SET_SIZE));
        rolledBackTransactionIds.set(new LRUCacheMap<>(TRANSACTION_ID_SET_SIZE));
        errorCount.set(0);
        warningCount.set(0);
        scnFreezeCount.set(0);
    }

    /**
     * Sets the current SCN.
     *
     * @param scn the current system change number
     */
    public void setCurrentScn(Scn scn) {
        currentScn.set(scn);
    }

    /**
     * Sets the current log file names.
     *
     * @param names the set of log file names
     */
    public void setCurrentLogFileName(Set<String> names) {
        currentLogFileName.set(names.stream().toArray(String[]::new));
        if (names.size() < minimumLogsMined.get()) {
            minimumLogsMined.set(names.size());
        }
        else if (minimumLogsMined.get() == 0) {
            minimumLogsMined.set(names.size());
        }
        if (names.size() > maximumLogsMined.get()) {
            maximumLogsMined.set(names.size());
        }
    }

    @Override
    /** {@inheritDoc} */
    public long getMinimumMinedLogCount() {
        return minimumLogsMined.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getMaximumMinedLogCount() {
        return maximumLogsMined.get();
    }

    /**
     * Sets the redo log status.
     *
     * @param status the status map
     */
    public void setRedoLogStatus(Map<String, String> status) {
        String[] statusArray = status.entrySet().stream().map(e -> e.getKey() + " | " + e.getValue()).toArray(String[]::new);
        redoLogStatus.set(statusArray);
    }

    /**
     * Sets the switch count.
     *
     * @param counter the switch counter
     */
    public void setSwitchCount(int counter) {
        switchCounter.set(counter);
    }

    /**
     * Sets the last captured DML count.
     *
     * @param dmlCount the DML count
     */
    public void setLastCapturedDmlCount(int dmlCount) {
        lastCapturedDmlCount.set(dmlCount);
        if (dmlCount > maxCapturedDmlCount.get()) {
            maxCapturedDmlCount.set(dmlCount);
        }
        totalCapturedDmlCount.getAndAdd(dmlCount);
    }

    /**
     * Sets the last duration of batch capturing.
     *
     * @param lastDuration the capturing duration
     */
    public void setLastDurationOfBatchCapturing(Duration lastDuration) {
        lastDurationOfFetchingQuery.set(lastDuration);
        totalDurationOfFetchingQuery.accumulateAndGet(lastDurationOfFetchingQuery.get(), Duration::plus);
        if (maxDurationOfFetchingQuery.get().toMillis() < lastDurationOfFetchingQuery.get().toMillis()) {
            maxDurationOfFetchingQuery.set(lastDuration);
        }
        logMinerQueryCount.incrementAndGet();
    }

    /**
     * Sets the last duration of batch processing.
     *
     * @param lastDuration the processing duration
     */
    public void setLastDurationOfBatchProcessing(Duration lastDuration) {
        lastBatchProcessingDuration.set(lastDuration);
        totalBatchProcessingDuration.accumulateAndGet(lastDuration, Duration::plus);
        if (maxBatchProcessingTime.get().toMillis() < lastDuration.toMillis()) {
            maxBatchProcessingTime.set(lastDuration);
        }
        if (minBatchProcessingTime.get().toMillis() > lastDuration.toMillis()) {
            minBatchProcessingTime.set(lastDuration);
        }
        else if (minBatchProcessingTime.get().toMillis() == 0L) {
            minBatchProcessingTime.set(lastDuration);
        }
        if (getLastBatchProcessingThroughput() > maxBatchProcessingThroughput.get()) {
            maxBatchProcessingThroughput.set(getLastBatchProcessingThroughput());
        }
    }

    /**
     * Increments the network connection problems counter.
     */
    public void incrementNetworkConnectionProblemsCounter() {
        networkConnectionProblemsCounter.incrementAndGet();
    }

    @Override
    /** {@inheritDoc} */
    public String getCurrentScn() {
        return currentScn.get().toString();
    }

    @Override
    /** {@inheritDoc} */
    public long getTotalCapturedDmlCount() {
        return totalCapturedDmlCount.get();
    }

    @Override
    /** {@inheritDoc} */
    public String[] getCurrentRedoLogFileName() {
        return currentLogFileName.get();
    }

    @Override
    /** {@inheritDoc} */
    public String[] getRedoLogStatus() {
        return redoLogStatus.get();
    }

    @Override
    /** {@inheritDoc} */
    public int getSwitchCounter() {
        return switchCounter.get();
    }

    @Override
    /** {@inheritDoc} */
    public Long getLastDurationOfFetchQueryInMilliseconds() {
        return lastDurationOfFetchingQuery.get() == null ? 0 : lastDurationOfFetchingQuery.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public long getLastBatchProcessingTimeInMilliseconds() {
        return lastBatchProcessingDuration.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public Long getMaxDurationOfFetchQueryInMilliseconds() {
        return maxDurationOfFetchingQuery.get() == null ? 0 : maxDurationOfFetchingQuery.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public Long getMaxCapturedDmlInBatch() {
        return maxCapturedDmlCount.get();
    }

    @Override
    /** {@inheritDoc} */
    public int getLastCapturedDmlCount() {
        return lastCapturedDmlCount.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getTotalProcessedRows() {
        return totalProcessedRows.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getTotalResultSetNextTimeInMilliseconds() {
        return totalResultSetNextTime.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public long getAverageBatchProcessingThroughput() {
        if (totalBatchProcessingDuration.get().isZero()) {
            return 0L;
        }
        return Math.round((totalCapturedDmlCount.floatValue() / totalBatchProcessingDuration.get().toMillis()) * 1000);
    }

    @Override
    /** {@inheritDoc} */
    public long getLastBatchProcessingThroughput() {
        if (lastBatchProcessingDuration.get().isZero()) {
            return 0L;
        }
        return Math.round((lastCapturedDmlCount.floatValue() / lastBatchProcessingDuration.get().toMillis()) * 1000);
    }

    @Override
    /** {@inheritDoc} */
    public long getFetchingQueryCount() {
        return logMinerQueryCount.get();
    }

    @Override
    /** {@inheritDoc} */
    public int getBatchSize() {
        return batchSize.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getMillisecondToSleepBetweenMiningQuery() {
        return millisecondToSleepBetweenMiningQuery.get();
    }

    @Override
    /** {@inheritDoc} */
    public int getHoursToKeepTransactionInBuffer() {
        return (int) keepTransactionsDuration.get().toHours();
    }

    @Override
    /** {@inheritDoc} */
    public long getMillisecondsToKeepTransactionsInBuffer() {
        return keepTransactionsDuration.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public long getMaxBatchProcessingThroughput() {
        return maxBatchProcessingThroughput.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getNetworkConnectionProblemsCounter() {
        return networkConnectionProblemsCounter.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getTotalParseTimeInMilliseconds() {
        return totalParseTime.get().toMillis();
    }

    /**
     * Adds the current parse time to the total.
     *
     * @param currentParseTime the parse time duration
     */
    public void addCurrentParseTime(Duration currentParseTime) {
        totalParseTime.accumulateAndGet(currentParseTime, Duration::plus);
    }

    @Override
    /** {@inheritDoc} */
    public long getTotalMiningSessionStartTimeInMilliseconds() {
        return totalStartLogMiningSessionDuration.get().toMillis();
    }

    /**
     * Adds the current mining session start time.
     *
     * @param currentStartLogMiningSession the start time duration
     */
    public void addCurrentMiningSessionStart(Duration currentStartLogMiningSession) {
        lastStartLogMiningSessionDuration.set(currentStartLogMiningSession);
        if (currentStartLogMiningSession.compareTo(maxStartingLogMiningSessionDuration.get()) > 0) {
            maxStartingLogMiningSessionDuration.set(currentStartLogMiningSession);
        }
        totalStartLogMiningSessionDuration.accumulateAndGet(currentStartLogMiningSession, Duration::plus);
    }

    @Override
    /** {@inheritDoc} */
    public long getLastMiningSessionStartTimeInMilliseconds() {
        return lastStartLogMiningSessionDuration.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public long getMaxMiningSessionStartTimeInMilliseconds() {
        return maxStartingLogMiningSessionDuration.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public long getTotalProcessingTimeInMilliseconds() {
        return totalProcessingTime.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public long getMinBatchProcessingTimeInMilliseconds() {
        return minBatchProcessingTime.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public long getMaxBatchProcessingTimeInMilliseconds() {
        return maxBatchProcessingTime.get().toMillis();
    }

    /**
     * Sets the current batch processing time.
     *
     * @param currentBatchProcessingTime the processing duration
     */
    public void setCurrentBatchProcessingTime(Duration currentBatchProcessingTime) {
        totalProcessingTime.accumulateAndGet(currentBatchProcessingTime, Duration::plus);
        setLastDurationOfBatchProcessing(currentBatchProcessingTime);
    }

    /**
     * Adds the current result set next time.
     *
     * @param currentNextTime the next time duration
     */
    public void addCurrentResultSetNext(Duration currentNextTime) {
        totalResultSetNextTime.accumulateAndGet(currentNextTime, Duration::plus);
    }

    /**
     * Adds processed rows to the total.
     *
     * @param rows the number of rows
     */
    public void addProcessedRows(Long rows) {
        totalProcessedRows.getAndAdd(rows);
    }

    @Override
    /** {@inheritDoc} */
    public void setBatchSize(int size) {
        if (size >= batchSizeMin && size <= batchSizeMax) {
            batchSize.set(size);
        }
    }

    @Override
    /** {@inheritDoc} */
    public void setMillisecondToSleepBetweenMiningQuery(long milliseconds) {
        if (milliseconds >= sleepTimeMin && milliseconds < sleepTimeMax) {
            millisecondToSleepBetweenMiningQuery.set(milliseconds);
        }
    }

    @Override
    /** {@inheritDoc} */
    public void changeSleepingTime(boolean increment) {
        long sleepTime = millisecondToSleepBetweenMiningQuery.get();
        if (increment && sleepTime < sleepTimeMax) {
            sleepTime = millisecondToSleepBetweenMiningQuery.addAndGet(sleepTimeIncrement);
        }
        else if (sleepTime > sleepTimeMin) {
            sleepTime = millisecondToSleepBetweenMiningQuery.addAndGet(-sleepTimeIncrement);
        }

        LOGGER.debug("Updating sleep time window. Sleep time {}. Min sleep time {}. Max sleep time {}.", sleepTime, sleepTimeMin, sleepTimeMax);
    }

    @Override
    /** {@inheritDoc} */
    public void changeBatchSize(boolean increment, boolean lobEnabled) {

        int currentBatchSize = batchSize.get();
        boolean incremented = false;
        if (increment && currentBatchSize < batchSizeMax) {
            currentBatchSize = batchSize.addAndGet(batchSizeMin);
            incremented = true;
        }
        else if (!increment && currentBatchSize > batchSizeMin) {
            currentBatchSize = batchSize.addAndGet(-batchSizeMin);
        }

        if (incremented && currentBatchSize == batchSizeMax) {
            if (!lobEnabled) {
                LOGGER.info("The connector is now using the maximum batch size {} when querying the LogMiner view. This could be indicative of large SCN gaps",
                        currentBatchSize);
            }
            else {
                LOGGER.info("The connector is now using the maximum batch size {} when querying the LogMiner view.", currentBatchSize);
            }
        }
        else {
            LOGGER.debug("Updating batch size window. Batch size {}. Min batch size {}. Max batch size {}.", currentBatchSize, batchSizeMin, batchSizeMax);
        }
    }

    // transactional buffer metrics

    @Override
    /** {@inheritDoc} */
    public long getNumberOfActiveTransactions() {
        return activeTransactions.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getNumberOfRolledBackTransactions() {
        return rolledBackTransactions.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getNumberOfCommittedTransactions() {
        return committedTransactions.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getNumberOfOversizedTransactions() {
        return oversizedTransactions.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getCommitThroughput() {
        long timeSpent = Duration.between(startTime, clock.instant()).toMillis();
        return committedTransactions.get() * MILLIS_PER_SECOND / (timeSpent != 0 ? timeSpent : 1);
    }

    @Override
    /** {@inheritDoc} */
    public long getRegisteredDmlCount() {
        return registeredDmlCount.get();
    }

    @Override
    /** {@inheritDoc} */
    public String getOldestScn() {
        return oldestScn.get().toString();
    }

    @Override
    /** {@inheritDoc} */
    public String getCommittedScn() {
        return committedScn.get().toString();
    }

    @Override
    /** {@inheritDoc} */
    public String getOffsetScn() {
        return offsetScn.get().toString();
    }

    @Override
    /** {@inheritDoc} */
    public long getLagFromSourceInMilliseconds() {
        return lagFromTheSourceDuration.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public long getMaxLagFromSourceInMilliseconds() {
        return maxLagFromTheSourceDuration.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public long getMinLagFromSourceInMilliseconds() {
        return minLagFromTheSourceDuration.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public Set<String> getAbandonedTransactionIds() {
        return abandonedTransactionIds.get().keySet();
    }

    @Override
    /** {@inheritDoc} */
    public Set<String> getRolledBackTransactionIds() {
        return rolledBackTransactionIds.get().keySet();
    }

    @Override
    /** {@inheritDoc} */
    public long getLastCommitDurationInMilliseconds() {
        return lastCommitDuration.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public long getMaxCommitDurationInMilliseconds() {
        return maxCommitDuration.get().toMillis();
    }

    @Override
    /** {@inheritDoc} */
    public int getErrorCount() {
        return errorCount.get();
    }

    @Override
    /** {@inheritDoc} */
    public int getWarningCount() {
        return warningCount.get();
    }

    @Override
    /** {@inheritDoc} */
    public int getScnFreezeCount() {
        return scnFreezeCount.get();
    }

    @Override
    /** {@inheritDoc} */
    public int getUnparsableDdlCount() {
        return unparsableDdlCount.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getMiningSessionUserGlobalAreaMemoryInBytes() {
        return miningSessionUserGlobalAreaMemory.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getMiningSessionUserGlobalAreaMaxMemoryInBytes() {
        return miningSessionUserGlobalAreaMaxMemory.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getMiningSessionProcessGlobalAreaMemoryInBytes() {
        return miningSessionProcessGlobalAreaMemory.get();
    }

    @Override
    /** {@inheritDoc} */
    public long getMiningSessionProcessGlobalAreaMaxMemoryInBytes() {
        return miningSessionProcessGlobalAreaMaxMemory.get();
    }

    /**
     * Sets the oldest SCN.
     *
     * @param scn the oldest system change number
     */
    public void setOldestScn(Scn scn) {
        oldestScn.set(scn);
    }

    /**
     * Sets the committed SCN.
     *
     * @param scn the committed system change number
     */
    public void setCommittedScn(Scn scn) {
        committedScn.set(scn);
    }

    /**
     * Sets the offset SCN.
     *
     * @param scn the offset system change number
     */
    public void setOffsetScn(Scn scn) {
        offsetScn.set(scn);
    }

    /**
     * Sets the active transaction count.
     *
     * @param activeTransactionCount the number of active transactions
     */
    public void setActiveTransactions(long activeTransactionCount) {
        activeTransactions.set(activeTransactionCount);
    }

    /**
     * Increments the rolled back transaction count.
     */
    public void incrementRolledBackTransactions() {
        rolledBackTransactions.incrementAndGet();
    }

    /**
     * Increments the committed transaction count.
     */
    public void incrementCommittedTransactions() {
        committedTransactions.incrementAndGet();
    }

    /**
     * Increments the oversized transaction count.
     */
    public void incrementOversizedTransactions() {
        oversizedTransactions.incrementAndGet();
    }

    /**
     * Increments the registered DML count.
     */
    public void incrementRegisteredDmlCount() {
        registeredDmlCount.incrementAndGet();
    }

    /**
     * Increments the committed DML count.
     *
     * @param counter the number of DML operations to add
     */
    public void incrementCommittedDmlCount(long counter) {
        committedDmlCount.getAndAdd(counter);
    }

    /**
     * Increments the error count.
     */
    public void incrementErrorCount() {
        errorCount.incrementAndGet();
    }

    /**
     * Increments the warning count.
     */
    public void incrementWarningCount() {
        warningCount.incrementAndGet();
    }

    /**
     * Increments the SCN freeze count.
     */
    public void incrementScnFreezeCount() {
        scnFreezeCount.incrementAndGet();
    }

    /**
     * Adds an abandoned transaction ID.
     *
     * @param transactionId the transaction ID
     */
    public void addAbandonedTransactionId(String transactionId) {
        if (transactionId != null) {
            abandonedTransactionIds.get().put(transactionId, transactionId);
        }
    }

    /**
     * Adds a rolled back transaction ID.
     *
     * @param transactionId the transaction ID
     */
    public void addRolledBackTransactionId(String transactionId) {
        if (transactionId != null) {
            rolledBackTransactionIds.get().put(transactionId, transactionId);
        }
    }

    /**
     * Sets the last commit duration.
     *
     * @param lastDuration the commit duration
     */
    public void setLastCommitDuration(Duration lastDuration) {
        lastCommitDuration.set(lastDuration);
        if (lastDuration.toMillis() > maxCommitDuration.get().toMillis()) {
            maxCommitDuration.set(lastDuration);
        }
    }

    /**
     * Calculates the time difference between the database server and the connector.
     * Along with the time difference also the offset of the database server time to UTC is stored.
     * Both values are required to calculate lag metrics.
     *
     * @param databaseSystemTime the system time (<code>SYSTIMESTAMP</code>) of the database
     */
    /**
     * Calculates the time difference between the database server and the connector.
     *
     * @param databaseSystemTime the system time of the database
     */
    public void calculateTimeDifference(OffsetDateTime databaseSystemTime) {
        this.zoneOffset.set(databaseSystemTime.getOffset());
        LOGGER.trace("Timezone offset of database system time is {} seconds", zoneOffset.get().getTotalSeconds());

        Instant now = clock.instant();
        long timeDiffMillis = Duration.between(databaseSystemTime.toInstant(), now).toMillis();
        this.timeDifference.set(timeDiffMillis);
        LOGGER.trace("Current time {} ms, database difference {} ms", now.toEpochMilli(), timeDiffMillis);
    }

    /**
     * Returns the database offset.
     *
     * @return the zone offset of the database
     */
    public ZoneOffset getDatabaseOffset() {
        return zoneOffset.get();
    }

    /**
     * Calculates the lag metrics.
     *
     * @param changeTime the change timestamp
     */
    public void calculateLagMetrics(Instant changeTime) {
        if (changeTime != null) {
            final Instant correctedChangeTime = changeTime.plusMillis(timeDifference.longValue()).minusSeconds(zoneOffset.get().getTotalSeconds());
            final Duration lag = Duration.between(correctedChangeTime, clock.instant()).abs();
            lagFromTheSourceDuration.set(lag);

            if (maxLagFromTheSourceDuration.get().toMillis() < lag.toMillis()) {
                maxLagFromTheSourceDuration.set(lag);
            }
            if (minLagFromTheSourceDuration.get().toMillis() > lag.toMillis()) {
                minLagFromTheSourceDuration.set(lag);
            }
            else if (minLagFromTheSourceDuration.get().toMillis() == 0) {
                minLagFromTheSourceDuration.set(lag);
            }
        }
    }

    /**
     * Increments the unparsable DDL count.
     */
    public void incrementUnparsableDdlCount() {
        unparsableDdlCount.incrementAndGet();
    }

    /**
     * Sets the user global area memory metrics.
     *
     * @param ugaMemory the UGA memory usage
     * @param ugaMaxMemory the maximum UGA memory usage
     */
    public void setUserGlobalAreaMemory(long ugaMemory, long ugaMaxMemory) {
        miningSessionUserGlobalAreaMemory.set(ugaMemory);
        if (ugaMaxMemory > miningSessionUserGlobalAreaMaxMemory.get()) {
            miningSessionUserGlobalAreaMaxMemory.set(ugaMaxMemory);
        }
    }

    /**
     * Sets the process global area memory metrics.
     *
     * @param pgaMemory the PGA memory usage
     * @param pgaMaxMemory the maximum PGA memory usage
     */
    public void setProcessGlobalAreaMemory(long pgaMemory, long pgaMaxMemory) {
        miningSessionProcessGlobalAreaMemory.set(pgaMemory);
        if (pgaMemory > miningSessionProcessGlobalAreaMaxMemory.get()) {
            miningSessionProcessGlobalAreaMaxMemory.set(pgaMemory);
        }
    }

    @Override
    /** {@inheritDoc} */
    public String toString() {
        return "YashanDbStreamingChangeEventSourceMetrics{" +
                "currentScn=" + currentScn +
                ", oldestScn=" + oldestScn.get() +
                ", committedScn=" + committedScn.get() +
                ", offsetScn=" + offsetScn.get() +
                ", logMinerQueryCount=" + logMinerQueryCount +
                ", totalProcessedRows=" + totalProcessedRows +
                ", totalCapturedDmlCount=" + totalCapturedDmlCount +
                ", totalDurationOfFetchingQuery=" + totalDurationOfFetchingQuery +
                ", lastCapturedDmlCount=" + lastCapturedDmlCount +
                ", lastDurationOfFetchingQuery=" + lastDurationOfFetchingQuery +
                ", maxCapturedDmlCount=" + maxCapturedDmlCount +
                ", maxDurationOfFetchingQuery=" + maxDurationOfFetchingQuery +
                ", totalBatchProcessingDuration=" + totalBatchProcessingDuration +
                ", lastBatchProcessingDuration=" + lastBatchProcessingDuration +
                ", maxBatchProcessingThroughput=" + maxBatchProcessingThroughput +
                ", currentLogFileName=" + Arrays.asList(currentLogFileName.get()) +
                ", minLogFilesMined=" + minimumLogsMined +
                ", maxLogFilesMined=" + maximumLogsMined +
                ", redoLogStatus=" + Arrays.asList(redoLogStatus.get()) +
                ", switchCounter=" + switchCounter +
                ", batchSize=" + batchSize +
                ", millisecondToSleepBetweenMiningQuery=" + millisecondToSleepBetweenMiningQuery +
                ", keepTransactionsDuration=" + keepTransactionsDuration.get() +
                ", networkConnectionProblemsCounter" + networkConnectionProblemsCounter +
                ", batchSizeDefault=" + batchSizeDefault +
                ", batchSizeMin=" + batchSizeMin +
                ", batchSizeMax=" + batchSizeMax +
                ", sleepTimeDefault=" + sleepTimeDefault +
                ", sleepTimeMin=" + sleepTimeMin +
                ", sleepTimeMax=" + sleepTimeMax +
                ", sleepTimeIncrement=" + sleepTimeIncrement +
                ", totalParseTime=" + totalParseTime +
                ", totalStartLogMiningSessionDuration=" + totalStartLogMiningSessionDuration +
                ", lastStartLogMiningSessionDuration=" + lastStartLogMiningSessionDuration +
                ", maxStartLogMiningSessionDuration=" + maxStartingLogMiningSessionDuration +
                ", totalProcessTime=" + totalProcessingTime +
                ", minBatchProcessTime=" + minBatchProcessingTime +
                ", maxBatchProcessTime=" + maxBatchProcessingTime +
                ", totalResultSetNextTime=" + totalResultSetNextTime +
                ", lagFromTheSource=Duration" + lagFromTheSourceDuration.get() +
                ", maxLagFromTheSourceDuration=" + maxLagFromTheSourceDuration.get() +
                ", minLagFromTheSourceDuration=" + minLagFromTheSourceDuration.get() +
                ", lastCommitDuration=" + lastCommitDuration +
                ", maxCommitDuration=" + maxCommitDuration +
                ", activeTransactions=" + activeTransactions.get() +
                ", rolledBackTransactions=" + rolledBackTransactions.get() +
                ", oversizedTransactions=" + oversizedTransactions.get() +
                ", committedTransactions=" + committedTransactions.get() +
                ", abandonedTransactionIds=" + abandonedTransactionIds.get() +
                ", rolledbackTransactionIds=" + rolledBackTransactionIds.get() +
                ", registeredDmlCount=" + registeredDmlCount.get() +
                ", committedDmlCount=" + committedDmlCount.get() +
                ", errorCount=" + errorCount.get() +
                ", warningCount=" + warningCount.get() +
                ", scnFreezeCount=" + scnFreezeCount.get() +
                ", unparsableDdlCount=" + unparsableDdlCount.get() +
                ", miningSessionUserGlobalAreaMemory=" + miningSessionUserGlobalAreaMemory.get() +
                ", miningSessionUserGlobalAreaMaxMemory=" + miningSessionUserGlobalAreaMaxMemory.get() +
                ", miningSessionProcessGlobalAreaMemory=" + miningSessionProcessGlobalAreaMemory.get() +
                ", miningSessionProcessGlobalAreaMaxMemory=" + miningSessionProcessGlobalAreaMaxMemory.get() +
                '}';
    }
}
