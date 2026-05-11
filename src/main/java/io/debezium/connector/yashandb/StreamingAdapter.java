/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.sql.SQLException;

import io.debezium.config.Configuration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.Clock;

/**
 * Contract that defines unique behavior for each possible {@code connection.adapter}.
 */
public interface StreamingAdapter {

    /**
     * Controls whether table names are viewed as case-sensitive or not.
     */
    enum TableNameCaseSensitivity {
        /**
         * Sensitive case implies that the table names are taken from the JDBC driver and kept as-is
         * in the in-memory relational objects.  Any {@link TableId} that is obtained will always
         * have a table-name in the case that the driver provided.  This is the default behavior
         * for almost all cases.
         */
        SENSITIVE,

        /**
         * Insensitive case implies that the table names are taken from the JDBC driver and converted
         * to lower-case in the in-memory relational objects.  Any {@link TableId} that is obtained
         * will always have a table-name in lower case regardless of how it may be represented in
         * the database.
         */
        INSENSITIVE
    };


    /**
     * Returns the type of this streaming adapter.
     *
     * @return the adapter type string
     */
    /**
     * Returns the type identifier of this streaming adapter.
     *
     * @return the adapter type string
     */
    String getType();


    /**
     * Returns the history record comparator for schema history.
     *
     * @return the history record comparator
     */
    /**
     * Returns the comparator used for comparing history records.
     *
     * @return the history record comparator
     */
    HistoryRecordComparator getHistoryRecordComparator();


    /**
     * Returns the offset context loader for reconstructing offsets from storage.
     *
     * @return the offset context loader
     */
    /**
     * Returns the loader for deserializing offset context from stored offsets.
     *
     * @return the offset context loader
     */
    OffsetContext.Loader<YashanDbOffsetContext> getOffsetContextLoader();

    /**
     * Creates and returns a streaming change event source.
     *
     * @param connection the YashanDB connection
     *
     * @param dispatcher the event dispatcher
     *
     * @param errorHandler the error handler
     *
     * @param clock the clock for time-based operations
     *
     * @param schema the database schema
     *
     * @param taskContext the task context
     *
     * @param jdbcConfig the JDBC configuration
     *
     * @param streamingMetrics the streaming metrics
     *
     * @return the streaming change event source
     */
    StreamingChangeEventSource<YashanDbPartition, YashanDbOffsetContext> getSource(YashanDbConnection connection,
                                                                                   EventDispatcher<YashanDbPartition, TableId> dispatcher,
                                                                                   ErrorHandler errorHandler, Clock clock,
                                                                                   YashanDbDatabaseSchema schema,
                                                                                   YashanDbTaskContext taskContext,
                                                                                   Configuration jdbcConfig,
                                                                                   YashanDbStreamingChangeEventSourceMetrics streamingMetrics);

    /**
     * Returns whether table names are case sensitive.
     *
     * By default the YashanDB driver returns table names that are case sensitive.  The table names will
     * be returned in upper-case by default and will only be returned in lower or mixed case when the
     * table is created using double-quotes to preserve case.  The adapter aligns with the driver's
     * behavior and enforces that table names are case sensitive by default.
     *
     * @param connection database connection, should never be {@code null}
     * @return the case sensitivity setting for table names used by the connector's runtime adapter
     */
    default TableNameCaseSensitivity getTableNameCaseSensitivity(YashanDbConnection connection) {
        return TableNameCaseSensitivity.SENSITIVE;
    }

    /**
     * Returns the offset context based on the snapshot state.
     *
     * @param ctx the relational snapshot context, should never be {@code null}
     * @param connectorConfig the connector configuration, should never be {@code null}
     * @param connection the database connection, should never be {@code null}
     * @return the offset context, never {@code null}
     * @throws SQLException if a database error occurred
     */
    /**
     * Determines the starting offset for streaming after a snapshot, based on the snapshot state.
     *
     * @param ctx the relational snapshot context, should never be {@code null}
     * @param connectorConfig the connector configuration, should never be {@code null}
     * @param connection the database connection, should never be {@code null}
     * @return the offset context for streaming, never {@code null}
     * @throws SQLException if a database error occurred
     */
    YashanDbOffsetContext determineSnapshotOffset(RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> ctx,
                                                  YashanDbConnectorConfig connectorConfig, YashanDbConnection connection)
            throws SQLException;
}
