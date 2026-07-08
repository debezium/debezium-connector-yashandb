/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sics.ystream.YstreamClientBoot;
import com.sics.ystream.conf.StartMode;
import com.sics.ystream.conf.YstreamConfig;
import com.sics.ystream.result.Position;

import io.debezium.connector.yashandb.YashanDbConnection;
import io.debezium.connector.yashandb.YashanDbConnectorConfig;
import io.debezium.connector.yashandb.YashanDbDatabaseSchema;
import io.debezium.connector.yashandb.YashanDbOffsetContext;
import io.debezium.connector.yashandb.YashanDbPartition;
import io.debezium.connector.yashandb.YashanDbStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * A {@link StreamingChangeEventSource} based on YashanDB's YStream API. The YStream event handler loop is executed in a
 * separate executor.
 */
public class YStreamStreamingChangeEventSource implements StreamingChangeEventSource<YashanDbPartition, YashanDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YStreamStreamingChangeEventSource.class);

    private static final int YSTREAM_RETRY_MAX_ATTEMPTS = 30;
    private static final long YSTREAM_RETRY_BACKOFF_MS = 1000;

    private final YashanDbConnectorConfig connectorConfig;
    private final YashanDbConnection jdbcConnection;
    private final EventDispatcher<YashanDbPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final YashanDbDatabaseSchema schema;
    private final YashanDbStreamingChangeEventSourceMetrics streamingMetrics;
    private final String yStreamServerName;
    private YstreamClientBoot<YStreamRecord> ystreamClientBoot;
    /**
     * A message box between thread that is informed about committed offsets and the YStream thread.
     * When the last offset is committed its value is passed to the YStream thread and a watermark is
     * set to signal which events were safely processed.
     * This is important as setting watermark in a concurrent thread can lead to a deadlock due to an
     * internal YashanDB code locking.
     */
    private final AtomicReference<YStreamPosition> lcrMessage = new AtomicReference<>();
    private YashanDbOffsetContext effectiveOffset;
    private Position lastAppliedPosition = null;

    /**
     * Creates a YStreamStreamingChangeEventSource with the given configuration and dependencies.
     *
     * @param connectorConfig the connector configuration
     * @param jdbcConnection the JDBC connection
     * @param dispatcher the event dispatcher
     * @param errorHandler the error handler
     * @param clock the clock for timestamping
     * @param schema the database schema
     * @param streamingMetrics the streaming metrics
     */
    public YStreamStreamingChangeEventSource(YashanDbConnectorConfig connectorConfig, YashanDbConnection jdbcConnection,
                                             EventDispatcher<YashanDbPartition, TableId> dispatcher, ErrorHandler errorHandler,
                                             Clock clock, YashanDbDatabaseSchema schema,
                                             YashanDbStreamingChangeEventSourceMetrics streamingMetrics) {
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.streamingMetrics = streamingMetrics;
        this.yStreamServerName = connectorConfig.getYstreamServerName();
    }

    /**
     * Initializes the streaming event source with the given offset context.
     *
     * @param offsetContext the offset context to use, or null for a fresh start
     */
    @Override
    public void init(YashanDbOffsetContext offsetContext) throws InterruptedException {
        this.effectiveOffset = offsetContext == null ? emptyContext() : offsetContext;
    }

    /**
     * Creates an empty offset context with default values.
     *
     * @return a new empty YashanDbOffsetContext
     */
    private YashanDbOffsetContext emptyContext() {
        return YashanDbOffsetContext.create().logicalName(connectorConfig)
                .snapshotPendingTransactions(Collections.emptyMap())
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>()).build();
    }

    /**
     * Executes the streaming loop, receiving and processing YStream events until the context is stopped.
     *
     * @param context the change event source context
     * @param partition the YashanDB partition
     * @param offsetContext the offset context
     */
    @Override
    public void execute(ChangeEventSourceContext context, YashanDbPartition partition, YashanDbOffsetContext offsetContext)
            throws InterruptedException {

        this.effectiveOffset = offsetContext;
        YStreamEventHandler eventHandler = createEventHandler(partition, offsetContext);
        String serverName = connectorConfig.getYstreamServerName();
        LOGGER.info("YStream serverName: {}", serverName);
        LOGGER.info("Init YStream server");
        lastAppliedPosition = offsetContext.getLcrPosition();

        try {
            initializeYStreamConnection(offsetContext);

            // 2. receive events while running
            processEventLoop(context, partition, offsetContext, eventHandler);
        }
        catch (Throwable e) {
            handleExecutionError(e, context);
        }
        finally {
            closeYStreamClient();
        }
    }

    /**
     * Creates the event handler for processing YStream records.
     */
    private YStreamEventHandler createEventHandler(YashanDbPartition partition, YashanDbOffsetContext offsetContext) {
        return new YStreamEventHandler(connectorConfig, errorHandler, dispatcher, clock, schema,
                partition, offsetContext, this, streamingMetrics);
    }

    /**
     * Initializes the YStream connection with retry logic.
     */
    private void initializeYStreamConnection(YashanDbOffsetContext offsetContext) throws InterruptedException {
        ystreamClientBoot = YstreamClientBoot.getClient();
        openConnectionWithRetry(offsetContext, false);
    }

    /**
     * Opens YStream connection with configurable retry behavior.
     *
     * @param offsetContext the offset context for recovery position
     * @param isReopen whether this is a reconnection attempt
     * @throws InterruptedException if retry is interrupted
     */
    private void openConnectionWithRetry(YashanDbOffsetContext offsetContext, boolean isReopen) throws InterruptedException {
        boolean openRetryable = true;
        int openAttempt = 0;
        String operationType = isReopen ? "re-open" : "open";

        while (openRetryable && openAttempt < YSTREAM_RETRY_MAX_ATTEMPTS) {
            try {
                ystreamClientBoot.open(buildYStreamConfig(offsetContext));
                openRetryable = false;
                LOGGER.debug("YStream {} succeeded on attempt {}", operationType, openAttempt + 1);
            }
            catch (Exception e) {
                openAttempt++;
                LOGGER.warn("YStream {} failed (attempt {}/{}): {}",
                        operationType, openAttempt, YSTREAM_RETRY_MAX_ATTEMPTS, e.getMessage());

                if (openAttempt >= YSTREAM_RETRY_MAX_ATTEMPTS) {
                    throw new RuntimeException("YStream " + operationType + " failed after "
                            + YSTREAM_RETRY_MAX_ATTEMPTS + " attempts", e);
                }

                sleepWithInterruptedCheck(YSTREAM_RETRY_BACKOFF_MS);
            }
        }
    }

    /**
     * Builds the YStream configuration with current connector settings.
     */
    private YstreamConfig<YStreamRecord> buildYStreamConfig(YashanDbOffsetContext offsetContext) {
        return YstreamConfig.<YStreamRecord> builder()
                .setHost(jdbcConnection.config().getHostname())
                .setPort(String.valueOf(jdbcConnection.config().getPort()))
                .setUser(jdbcConnection.config().getUser())
                .setPassword(jdbcConnection.config().getPassword())
                .setDeserializer(new YStreamDeserializer())
                .setRecoverPosition(offsetContext.getRecoverPosition())
                .setStartMode(StartMode.RECOVER)
                .setPollTimeout(connectorConfig.getyStreamPollTimeout())
                .setClientResponseTimeout(connectorConfig.getyStreamClientResponseTimeout())
                .setQueueSize(connectorConfig.getyStreamQueueSize())
                .setServerName(yStreamServerName)
                .build();
    }

    /**
     * Main event processing loop - receives and processes YStream events.
     */
    private void processEventLoop(ChangeEventSourceContext context, YashanDbPartition partition,
                                  YashanDbOffsetContext offsetContext, YStreamEventHandler eventHandler)
            throws InterruptedException {

        while (context.isRunning()) {
            LOGGER.trace("Receiving LCR");
            YStreamRecord record = fetchNextRecord(offsetContext);

            if (record != null) {
                eventHandler.processRecord(record);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }

            handlePauseIfNeeded(context);
        }
    }

    /**
     * Fetches the next record from YStream with automatic reconnection on failure.
     */
    private YStreamRecord fetchNextRecord(YashanDbOffsetContext offsetContext) throws InterruptedException {
        YStreamRecord next = null;
        boolean nextRetryable = true;
        int nextAttempt = 0;

        while (nextRetryable && nextAttempt < YSTREAM_RETRY_MAX_ATTEMPTS) {
            try {
                next = ystreamClientBoot.next();
                nextRetryable = false;
            }
            catch (Exception e) {
                nextAttempt++;
                LOGGER.warn("YStream next() failed (attempt {}/{}), will re-open connection: {}",
                        nextAttempt, YSTREAM_RETRY_MAX_ATTEMPTS, e.getMessage());

                // Re-open connection on next() failure
                reopenConnectionWithRetry(offsetContext, nextAttempt);

                if (nextAttempt >= YSTREAM_RETRY_MAX_ATTEMPTS) {
                    throw new RuntimeException("YStream next() failed after "
                            + YSTREAM_RETRY_MAX_ATTEMPTS + " reconnection attempts", e);
                }

                sleepWithInterruptedCheck(YSTREAM_RETRY_BACKOFF_MS);
            }
        }
        return next;
    }

    /**
     * Reopens YStream connection after failure.
     */
    private void reopenConnectionWithRetry(YashanDbOffsetContext offsetContext, int attempt) {
        closeQuietly(ystreamClientBoot);
        try {
            ystreamClientBoot = YstreamClientBoot.getClient();
            openConnectionWithRetry(offsetContext, true);
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Reconnection interrupted", ie);
        }
        catch (Exception e) {
            LOGGER.warn("YStream re-open failed (attempt {}/{}): {}",
                    attempt, YSTREAM_RETRY_MAX_ATTEMPTS, e.getMessage());
        }
    }

    /**
     * Handles pause state if streaming is paused.
     */
    private void handlePauseIfNeeded(ChangeEventSourceContext context) throws InterruptedException {
        if (context.isPaused()) {
            LOGGER.info("Streaming will now pause");
            context.streamingPaused();
            context.waitSnapshotCompletion();
            LOGGER.info("Streaming resumed");
        }
    }

    /**
     * Handles execution errors, distinguishing between normal shutdown and actual errors.
     */
    private void handleExecutionError(Throwable e, ChangeEventSourceContext context) {
        if (context.isRunning()) {
            LOGGER.error("Streaming error occurred: {}", e.getMessage(), e);
            errorHandler.setProducerThrowable(e);
        }
        else {
            LOGGER.info("Exception caught during shutdown, ignoring: {}", e.getMessage());
        }
    }

    /**
     * Closes YStream client quietly, suppressing any exceptions.
     */
    private void closeQuietly(YstreamClientBoot<?> client) {
        if (client != null) {
            try {
                client.close();
            }
            catch (Exception closeEx) {
                LOGGER.debug("Error closing ystream client: {}", closeEx.getMessage());
            }
        }
    }

    /**
     * Closes YStream client in finally block.
     */
    private void closeYStreamClient() {
        closeQuietly(ystreamClientBoot);
    }

    /**
     * Sleep with proper interrupt handling.
     */
    private void sleepWithInterruptedCheck(long sleepMs) throws InterruptedException {
        try {
            Thread.sleep(sleepMs);
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new InterruptedException("Retry interrupted");
        }
    }

    /**
     * Commits the given offset to the YStream server for watermark tracking.
     *
     * @param partition the partition map
     * @param offset the offset map
     */
    @Override
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        if (ystreamClientBoot != null) {
            LOGGER.debug("Sending message to request recording of offsets to YashanDB");
            final YStreamPosition lcrPosition = YStreamPosition.valueOf(offset);
            // We can safely overwrite the message even if it was not processed. The watermarked will be set to the highest
            // (last) delivered value in a single step instead of incrementally
            sendPublishedPosition(lcrPosition);
        }
    }

    /**
     * @return the effective offset context
     */
    @Override
    public YashanDbOffsetContext getOffsetContext() {
        return effectiveOffset;
    }

    /**
     * @return the YStream client boot instance
     */
    public YstreamClientBoot<YStreamRecord> getYstreamClientBoot() {
        return ystreamClientBoot;
    }

    /**
     * Sends the published position to the message box for the YStream thread to process.
     *
     * @param lcrPosition the LCR position to publish
     */
    private void sendPublishedPosition(final YStreamPosition lcrPosition) {
        // Only advance the watermark when the committed position is strictly greater than the last applied position.
        // This prevents setting the initial startup position as the watermark and ensures monotonically increasing watermarks.
        if (lastAppliedPosition == null || lcrPosition.getLcrPosition().compareTo(lastAppliedPosition) > 0) {
            lastAppliedPosition = lcrPosition.getLcrPosition();
            lcrMessage.set(lcrPosition);
        }
    }

    /**
     * Retrieves and clears the published position from the message box.
     *
     * @return the published position and SCN, or null if none pending
     */
    YStreamPosition receivePublishedPosition() {
        return lcrMessage.getAndSet(null);
    }
}
