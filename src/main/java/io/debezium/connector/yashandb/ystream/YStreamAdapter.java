/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import java.sql.SQLException;
import java.util.Base64;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sics.ystream.result.LogPosition;
import com.sics.ystream.result.Position;
import com.sics.ystream.result.SystemChangeNumber;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.yashandb.AbstractStreamingAdapter;
import io.debezium.connector.yashandb.Scn;
import io.debezium.connector.yashandb.SourceInfo;
import io.debezium.connector.yashandb.YashanDbConnection;
import io.debezium.connector.yashandb.YashanDbConnectorConfig;
import io.debezium.connector.yashandb.YashanDbDatabaseSchema;
import io.debezium.connector.yashandb.YashanDbOffsetContext;
import io.debezium.connector.yashandb.YashanDbPartition;
import io.debezium.connector.yashandb.YashanDbStreamingChangeEventSourceMetrics;
import io.debezium.connector.yashandb.YashanDbTaskContext;
import io.debezium.document.Document;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.Clock;

/**
 * The streaming adapter implementation for YashanDB YStream.
 */
public class YStreamAdapter extends AbstractStreamingAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(YStreamAdapter.class);

    public static final String TYPE = "ystream";

    /**
     * Creates a YStreamAdapter with the given connector configuration.
     *
     * @param connectorConfig the YashanDB connector configuration
     */
    public YStreamAdapter(YashanDbConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    /**
     * @return the streaming adapter type identifier
     */
    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Returns a comparator that determines if one recorded position is at or before another.
     */
    @Override
    public HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            /**
             * Compares two document positions to determine if recorded is at or before desired.
             *
             * @param recorded the recorded position document
             * @param desired the desired position document
             * @return true if recorded is at or before desired
             */
            @Override
            public boolean isPositionAtOrBefore(Document recorded, Document desired) {
                final YStreamPosition recordedPosition = documentToPosition(recorded);
                final YStreamPosition desiredPosition = documentToPosition(desired);
                final Scn recordedScn = recordedPosition != null ? recordedPosition.getScn() : resolveScn(recorded);
                final Scn desiredScn = desiredPosition != null ? desiredPosition.getScn() : resolveScn(desired);
                if (recordedPosition != null && desiredPosition != null) {
                    return recordedPosition.compareTo(desiredPosition) < 1;
                }
                return recordedScn.compareTo(desiredScn) < 1;
            }
        };
    }

    /**
     * Converts a document containing position data into a YStreamPosition.
     *
     * @param document the document containing position keys
     * @return a YStreamPosition constructed from the document
     */
    private static YStreamPosition documentToPosition(Document document) {
        long scn = document.getLong(SourceInfo.POSITION_SCN_KEY);
        String instanceId = document.getString(SourceInfo.INSTANCE_ID_KEY);
        long groupLsn = document.getLong(SourceInfo.GROUP_LSN_KEY);
        int groupOffset = document.getInteger(SourceInfo.GROUP_OFFSET_KEY);
        int batchRowId = document.getInteger(SourceInfo.BATCH_ROW_ID_KEY);
        if (isDigit(instanceId)) {
            return new YStreamPosition(new Position(new SystemChangeNumber(scn), new LogPosition(Byte.parseByte(instanceId), groupLsn, groupOffset, batchRowId)));
        }
        else {
            // Backward compatibility with legacy offset data (e.g., Base64-encoded instance ID like "AAAAAAA=")
            byte[] instanceIdBytes = Base64.getDecoder().decode(instanceId);
            return new YStreamPosition(new Position(new SystemChangeNumber(scn), new LogPosition(instanceIdBytes[0], groupLsn, groupOffset, batchRowId)));
        }
    }

    /**
     * Checks if the given string contains only digit characters.
     *
     * @param s the string to check
     * @return true if all characters are digits
     */
    private static boolean isDigit(String s) {
        for (char c : s.toCharArray()) {
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return the offset context loader for YStream
     */
    @Override
    public OffsetContext.Loader<YashanDbOffsetContext> getOffsetContextLoader() {
        return new YStreamOffsetContextLoader(connectorConfig);
    }

    /**
     * Creates and returns a new streaming change event source.
     *
     * @param connection the database connection
     * @param dispatcher the event dispatcher
     * @param errorHandler the error handler
     * @param clock the clock for timestamping
     * @param schema the database schema
     * @param taskContext the task context
     * @param jdbcConfig the JDBC configuration
     * @param streamingMetrics the streaming metrics
     * @return a new YStreamStreamingChangeEventSource
     */
    @Override
    public StreamingChangeEventSource<YashanDbPartition, YashanDbOffsetContext> getSource(YashanDbConnection connection,
                                                                                          EventDispatcher<YashanDbPartition, TableId> dispatcher,
                                                                                          ErrorHandler errorHandler,
                                                                                          Clock clock,
                                                                                          YashanDbDatabaseSchema schema,
                                                                                          YashanDbTaskContext taskContext,
                                                                                          Configuration jdbcConfig,
                                                                                          YashanDbStreamingChangeEventSourceMetrics streamingMetrics) {
        return new YStreamStreamingChangeEventSource(
                connectorConfig,
                connection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                streamingMetrics);
    }

    /**
     * @return the table name case sensitivity for the connection
     */
    @Override
    public TableNameCaseSensitivity getTableNameCaseSensitivity(YashanDbConnection connection) {
        return super.getTableNameCaseSensitivity(connection);
    }

    /**
     * Calculates the earliest active transaction SCN from the given values.
     *
     * @param st the start SCN
     * @param e1 the first end SCN, may be null
     * @param e2 the second end SCN, may be null
     * @return the minimum SCN among the provided values
     */
    private Scn calculateEarliestActiveTxnScn(
                                              Scn st, Scn e1, Scn e2) {
        // Select the minimum value among the three SCNs
        Scn res = st;
        if (e1 != null) {
            res = res.compareTo(e1) < 0 ? res : e1;
        }
        if (e2 != null) {
            res = res.compareTo(e2) < 0 ? res : e2;
        }
        return res;
    }

    /**
     * Determines the snapshot offset for the YStream adapter after an initial snapshot.
     *
     * @param ctx the snapshot context
     * @param connectorConfig the connector configuration
     * @param connection the database connection
     * @return the resolved offset context for streaming
     * @throws SQLException if there is a database error
     */
    @Override
    public YashanDbOffsetContext determineSnapshotOffset(RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> ctx,
                                                         YashanDbConnectorConfig connectorConfig,
                                                         YashanDbConnection connection)
            throws SQLException {

        final Optional<Scn> latestTableDdlScn = getLatestTableDdlScn(ctx, connection);

        // The oldest transaction SCN serves as the starting point for creation, and the second current SCN serves as the flashback point.
        Scn currentScn1;
        do {
            currentScn1 = connection.getCurrentScn();
        } while (areSameTimestamp(latestTableDdlScn.orElse(null), currentScn1, connection));
        Scn flashPointScn;
        try {
            TimeUnit.SECONDS.sleep(3);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DebeziumException("Interrupted in query earliestActiveTxnScn and current Scn2.");
        }

        do {
            flashPointScn = connection.getCurrentScn();
        } while (areSameTimestamp(latestTableDdlScn.orElse(null), flashPointScn, connection));

        Position position = new Position(
                new SystemChangeNumber(flashPointScn.add(Scn.valueOf(1)).longValue()), new LogPosition());

        LOGGER.info("\tCurrent SCN resolved as {}", flashPointScn);

        return YashanDbOffsetContext.create()
                .logicalName(connectorConfig)
                .ystreamStartScn(currentScn1)
                .recoverPosition(position)
                .scn(flashPointScn)
                .snapshotScn(flashPointScn)
                .snapshotPendingTransactions(Collections.emptyMap())
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>())
                .build();
    }
}
