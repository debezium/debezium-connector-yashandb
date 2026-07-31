/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sics.ystream.YstreamClientBoot;
import com.sics.ystream.conf.StartMode;
import com.sics.ystream.conf.YstreamConfig;
import com.sics.ystream.exception.YstreamException;
import com.sics.ystream.result.Position;

import io.debezium.DebeziumException;
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
import io.debezium.util.Metronome;

/**
 * A {@link StreamingChangeEventSource} based on YashanDB's YStream API. The YStream event handler loop is executed in a
 * separate executor.
 */
public class YStreamStreamingChangeEventSource implements StreamingChangeEventSource<YashanDbPartition, YashanDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YStreamStreamingChangeEventSource.class);

    private static final int YSTREAM_RETRY_MAX_ATTEMPTS = 30;
    private static final long YSTREAM_RETRY_BACKOFF_MS = 2000;
    private static final long YSTREAM_RETRY_WINDOW_MS = 180_000;

    private final YashanDbConnectorConfig connectorConfig;
    private final YashanDbConnection jdbcConnection;
    private final EventDispatcher<YashanDbPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final YashanDbDatabaseSchema schema;
    private final YashanDbStreamingChangeEventSourceMetrics streamingMetrics;
    private final String yStreamServerName;
    private YstreamClientBoot<YStreamRecord> ystreamClientBoot;
    private boolean ystreamClientOpen;
    private final Deque<Long> ystreamFailureTimestamps = new ArrayDeque<>();

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
            processEventLoop(context, partition, offsetContext, eventHandler);
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (Throwable e) {
            handleExecutionError(e, context);
        }
        finally {
            resetYStreamClient();
        }
    }

    private YStreamEventHandler createEventHandler(YashanDbPartition partition, YashanDbOffsetContext offsetContext) {
        return new YStreamEventHandler(connectorConfig, errorHandler, dispatcher, clock, schema,
                partition, offsetContext, this, streamingMetrics);
    }

    private void processEventLoop(ChangeEventSourceContext context, YashanDbPartition partition,
                                  YashanDbOffsetContext offsetContext, YStreamEventHandler eventHandler)
            throws InterruptedException {
        while (context.isRunning()) {
            LOGGER.trace("Receiving LCR");
            YStreamRecord next = fetchNextRecord(offsetContext);
            if (next != null) {
                eventHandler.processRecord(next);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }

            if (context.isPaused()) {
                LOGGER.info("Streaming will now pause");
                context.streamingPaused();
                context.waitSnapshotCompletion();
                LOGGER.info("Streaming resumed");
            }
        }
    }

    private YStreamRecord fetchNextRecord(YashanDbOffsetContext offsetContext) throws InterruptedException {
        Exception lastFailure = null;
        Metronome retryMetronome = null;

        while (ystreamFailureTimestamps.size() < YSTREAM_RETRY_MAX_ATTEMPTS) {
            try {
                ensureConnectionOpen(offsetContext);
                return ystreamClientBoot.next();
            }
            catch (InterruptedException e) {
                throw e;
            }
            catch (Exception e) {
                lastFailure = e;
                ystreamFailureTimestamps.addLast(clock.currentTimeInMillis());
                // Remove failures outside the time window
                long now = clock.currentTimeInMillis();
                while (!ystreamFailureTimestamps.isEmpty()
                        && now - ystreamFailureTimestamps.peekFirst() > YSTREAM_RETRY_WINDOW_MS) {
                    ystreamFailureTimestamps.pollFirst();
                }
                resetYStreamClient();

                LOGGER.warn("YStream read attempt {}/{} failed within last {}s: {}",
                        ystreamFailureTimestamps.size(), YSTREAM_RETRY_MAX_ATTEMPTS,
                        YSTREAM_RETRY_WINDOW_MS / 1000, e.getMessage());
                if (retryMetronome == null) {
                    retryMetronome = Metronome.sleeper(Duration.ofMillis(YSTREAM_RETRY_BACKOFF_MS), clock);
                }
                retryMetronome.pause();
            }
        }

        throw new DebeziumException("YStream read failed: "
                + YSTREAM_RETRY_MAX_ATTEMPTS + " attempts within "
                + (YSTREAM_RETRY_WINDOW_MS / 1000) + "s window", lastFailure);
    }

    private void ensureConnectionOpen(YashanDbOffsetContext offsetContext) throws YstreamException {
        if (ystreamClientBoot == null) {
            ystreamClientBoot = YstreamClientBoot.getClient();
        }
        if (!ystreamClientOpen) {
            ystreamClientBoot.open(buildYStreamConfig(offsetContext));
            ystreamClientOpen = true;
        }
    }

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

    private void resetYStreamClient() {
        if (ystreamClientBoot != null) {
            try {
                ystreamClientBoot.close();
            }
            catch (Exception e) {
                LOGGER.debug("Error closing YStream client: {}", e.getMessage());
            }
        }
        ystreamClientBoot = null;
        ystreamClientOpen = false;
    }

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
