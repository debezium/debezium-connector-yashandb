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

import io.debezium.connector.yashandb.Scn;
import io.debezium.connector.yashandb.SourceInfo;
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
    private final AtomicReference<PositionAndScn> lcrMessage = new AtomicReference<>();
    private YashanDbOffsetContext effectiveOffset;

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

    @Override
    public void init(YashanDbOffsetContext offsetContext) throws InterruptedException {
        this.effectiveOffset = offsetContext == null ? emptyContext() : offsetContext;
    }

    private YashanDbOffsetContext emptyContext() {
        return YashanDbOffsetContext.create().logicalName(connectorConfig)
                .snapshotPendingTransactions(Collections.emptyMap())
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>()).build();
    }

    @Override
    public void execute(ChangeEventSourceContext context, YashanDbPartition partition, YashanDbOffsetContext offsetContext)
            throws InterruptedException {

        this.effectiveOffset = offsetContext;
        YStreamEventHandler eventHandler = new YStreamEventHandler(connectorConfig, errorHandler, dispatcher, clock, schema,
                partition, offsetContext, this, streamingMetrics);
        // create
        String serverName = connectorConfig.getYstreamServerName();
        LOGGER.info("YStream serverName: {}", serverName);
        LOGGER.info("Init YStream server");

        try {
            try {
                // 1. connect
                ystreamClientBoot = YstreamClientBoot.getClient();
                ystreamClientBoot.open(
                        YstreamConfig.<YStreamRecord> builder()
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
                                .build());

                // 2. receive events while running
                while (context.isRunning()) {
                    LOGGER.trace("Receiving LCR");
                    YStreamRecord next = ystreamClientBoot.next();
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
            finally {
                ystreamClientBoot.close();
            }
        }
        catch (Throwable e) {
            errorHandler.setProducerThrowable(e);
        }
    }

    @Override
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        if (ystreamClientBoot != null) {
            LOGGER.debug("Sending message to request recording of offsets to YashanDB");
            final YStreamPosition lcrPosition = YStreamPosition.valueOf(offset);
            final Scn scn = YashanDbOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
            // We can safely overwrite the message even if it was not processed. The watermarked will be set to the highest
            // (last) delivered value in a single step instead of incrementally
            sendPublishedPosition(lcrPosition, scn);
        }
    }

    @Override
    public YashanDbOffsetContext getOffsetContext() {
        return effectiveOffset;
    }

    public static class PositionAndScn {
        public final YStreamPosition position;
        public final Scn scn;

        public PositionAndScn(YStreamPosition position, Scn scn) {
            this.position = position;
            this.scn = scn;
        }
    }

    public YstreamClientBoot<YStreamRecord> getYstreamClientBoot() {
        return ystreamClientBoot;
    }

    private void sendPublishedPosition(final YStreamPosition lcrPosition, final Scn scn) {
        if (lcrPosition.getRawPosition().compareTo(this.effectiveOffset.getRecoverPosition()) > 0) {
            lcrMessage.set(new PositionAndScn(lcrPosition, scn));
        }
    }

    PositionAndScn receivePublishedPosition() {
        return lcrMessage.getAndSet(null);
    }
}
