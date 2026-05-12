/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sics.ystream.result.Position;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;

/**
 * A {@link StreamingChangeEventSource} for YashanDB.
 */
public class YashanDbSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<YashanDbPartition, YashanDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDbSnapshotChangeEventSource.class);

    private final YashanDbConnectorConfig connectorConfig;
    private final YashanDbConnection jdbcConnection;
    private final YashanDbDatabaseSchema databaseSchema;

    public YashanDbSnapshotChangeEventSource(YashanDbConnectorConfig connectorConfig, MainConnectionProvidingConnectionFactory connectionFactory,
                                             YashanDbDatabaseSchema schema, EventDispatcher<YashanDbPartition, TableId> dispatcher, Clock clock,
                                             SnapshotProgressListener<YashanDbPartition> snapshotProgressListener,
                                             NotificationService<YashanDbPartition, YashanDbOffsetContext> notificationService, SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = (YashanDbConnection) connectionFactory.mainConnection();
        this.databaseSchema = schema;
    }

    @Override
    protected SnapshotContext<YashanDbPartition, YashanDbOffsetContext> prepare(YashanDbPartition partition, boolean onDemand) {

        return new YashanDbSnapshotContext(partition, connectorConfig.getDatabaseName(), onDemand);
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> ctx)
            throws Exception {
        return jdbcConnection.getAllTableIds(ctx.catalogName);
        // this very slow approach(commented out), it took 30 minutes on an instance with 600 tables
        // return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[] {"TABLE"} );
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> snapshotContext)
            throws SQLException, InterruptedException {
        if (connectorConfig.getSnapshotLockingMode().get().usesLocking()) {
            ((YashanDbSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("dbz_schema_snapshot");

            try (Statement statement = jdbcConnection.connection().createStatement()) {
                for (TableId tableId : snapshotContext.capturedTables) {
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while locking table " + tableId);
                    }

                    Optional<String> lockingStatement = snapshotterService.getSnapshotLock().tableLockingStatement(null, quote(tableId));
                    if (lockingStatement.isPresent()) {
                        LOGGER.debug("Locking table {}", tableId);
                        statement.execute(lockingStatement.get());
                    }
                }
            }
        }
        else {
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> snapshotContext)
            throws SQLException {
        if (connectorConfig.getSnapshotLockingMode().get().usesLocking()) {
            jdbcConnection.connection().rollback(((YashanDbSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
        }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> ctx,
                                           YashanDbOffsetContext previousOffset)
            throws Exception {

        if (previousOffset != null && !snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot()) {
            ctx.offset = previousOffset;
            tryStartingSnapshot(ctx);
            return;
        }

        ctx.offset = connectorConfig.getAdapter().determineSnapshotOffset(ctx, connectorConfig, jdbcConnection);
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> snapshotContext,
                                      YashanDbOffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws SQLException, InterruptedException {
        Set<TableId> capturedSchemaTables;
        if (databaseSchema.storeOnlyCapturedTables()) {
            capturedSchemaTables = snapshotContext.capturedTables;
            LOGGER.info("Only captured tables schema should be captured, capturing: {}", capturedSchemaTables);
        }
        else {
            capturedSchemaTables = snapshotContext.capturedSchemaTables;
            LOGGER.info("All eligible tables schema should be captured, capturing: {}", capturedSchemaTables);
        }

        Set<String> schemas = capturedSchemaTables.stream().map(TableId::schema).collect(Collectors.toSet());

        final Tables.TableFilter tableFilter = getTableFilter(snapshottingTask, snapshotContext);
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }
            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    null,
                    schema,
                    tableFilter,
                    null,
                    false);
        }
    }

    private Tables.TableFilter getTableFilter(SnapshottingTask snapshottingTask, RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> snapshotContext) {

        if (snapshottingTask.isOnDemand()) {
            return Tables.TableFilter.fromPredicate(snapshotContext.capturedTables::contains);
        }

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        // however, for users interested only in captured tables, we need to pass also table filter
        return connectorConfig.storeOnlyCapturedTables() ? connectorConfig.getTableFilters().dataCollectionFilter() : null;
    }

    @Override
    protected String enhanceOverriddenSelect(RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> snapshotContext,
                                             String overriddenSelect, TableId tableId) {
        String snapshotOffset = (String) snapshotContext.offset.getOffset().get(SourceInfo.SCN_KEY);
        String token = connectorConfig.getTokenToReplaceInSnapshotPredicate();
        if (token != null) {
            return overriddenSelect.replaceAll(token, " AS OF SCN " + snapshotOffset);
        }
        return overriddenSelect;
    }

    @Override
    protected Collection<TableId> getTablesForSchemaChange(RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> snapshotContext) {
        return snapshotContext.capturedSchemaTables;
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> snapshotContext,
                                                    Table table)
            throws SQLException {
        return SchemaChangeEvent.ofCreate(
                snapshotContext.partition,
                snapshotContext.offset,
                snapshotContext.catalogName,
                table.id().schema(),
                jdbcConnection.getTableMetadataDdl(table.id()),
                table,
                true);
    }

    @Override
    protected Instant getSnapshotSourceTimestamp(JdbcConnection jdbcConnection, YashanDbOffsetContext offset, TableId tableId) {
        try {
            final YashanDbConnection connection = (YashanDbConnection) jdbcConnection;
            return connection.getScnToTimestamp(offset.getScn())
                    .orElseThrow(() -> new ConnectException("Failed reading SCN timestamp from database"))
                    // Database host timezone adjustment
                    .minusSeconds(connection.getScnToTimestamp(offset.getSnapshotScn()).get().toEpochMilli())
                    // JVM timezone adjustment
                    .plusSeconds(ZoneId.systemDefault().getRules().getOffset(Instant.now()).getTotalSeconds());
        }
        catch (SQLException e) {
            throw new ConnectException("Failed reading SCN timestamp from source database", e);
        }
    }

    /**
     * Generate a valid YashanDB query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {

        return snapshotterService.getSnapshotQuery().snapshotQuery(quote(tableId), columns);
    }

    @Override
    protected List<Pattern> getSignalDataCollectionPattern(String signalingDataCollection) {
        // YashanDB expects this value to be supplied using "<database>.<schema>.<table>"; however the
        // TableIdMapper used by the connector uses only "<schema>.<table>". This primarily targets
        // a fix for this specific use case as a much larger refactor is likely necessary long term.
        final TableId tableId = TableId.parse(signalingDataCollection);
        return Strings.listOfRegex(tableId.schema() + "." + tableId.table(), Pattern.CASE_INSENSITIVE);
    }

    private String quote(TableId tableId) {
        return new TableId(null, tableId.schema(), tableId.table()).toDoubleQuotedString();
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class YashanDbSnapshotContext extends RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> {

        private Savepoint preSchemaSnapshotSavepoint;

        YashanDbSnapshotContext(YashanDbPartition partition, String catalogName, boolean onDemand) {
            super(partition, catalogName, onDemand);
        }
    }

    @Override
    protected YashanDbOffsetContext copyOffset(RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> snapshotContext) {
        return load(snapshotContext.offset.getOffset());
    }

    public YashanDbOffsetContext load(Map<String, ?> offset) {
        boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
        boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(YashanDbOffsetContext.SNAPSHOT_COMPLETED_KEY));
        boolean isCreateServer = Boolean.TRUE.equals(offset.get(YashanDbOffsetContext.YSTREAM_SERVER_CREATE));
        Scn scn = YashanDbOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        CommitScn commitScn = CommitScn.load(offset);
        Map<String, Scn> snapshotPendingTransactions = YashanDbOffsetContext.loadSnapshotPendingTransactions(offset);
        Scn snapshotScn = YashanDbOffsetContext.loadSnapshotScn(offset);
        Scn ystreamStartScn = YashanDbOffsetContext.loadYstreamStartScn(offset);
        Position recoverPosition = YashanDbOffsetContext.loadRecoverPosition(offset);
        return new YashanDbOffsetContext(connectorConfig, scn, commitScn, snapshotScn, ystreamStartScn, recoverPosition, snapshotPendingTransactions, snapshot,
                snapshotCompleted,
                TransactionContext.load(offset),
                SignalBasedIncrementalSnapshotContext.load(offset), isCreateServer);
    }

    @Override
    protected Callable<Void> createDataEventsForTableCallable(ChangeEventSourceContext sourceContext,
                                                              RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> snapshotContext,
                                                              EventDispatcher.SnapshotReceiver<YashanDbPartition> snapshotReceiver, Table table,
                                                              boolean firstTable, boolean lastTable, int tableOrder, int tableCount,
                                                              String selectStatement, OptionalLong rowCount, Set<TableId> rowCountKeySet,
                                                              Queue<JdbcConnection> connectionPool, Queue<YashanDbOffsetContext> offsets) {
        return () -> {
            JdbcConnection connection = connectionPool.poll();
            YashanDbOffsetContext offset = offsets.poll();
            try {
                final int maxRetries = getTableSnapshotMaxRetries();
                final Metronome retrySleeper = Metronome.sleeper(Duration.ofSeconds(5), clock);

                for (int i = 0; i <= maxRetries; i++) {
                    try {
                        doCreateDataEventsForTable(sourceContext, snapshotContext, offset, snapshotReceiver, table, firstTable,
                                lastTable, tableOrder, tableCount, selectStatement, rowCount, rowCountKeySet, connection);
                        break;
                    }
                    catch (SQLException e) {
                        notificationService.initialSnapshotNotificationService().notifyCompletedTableWithError(snapshotContext.partition,
                                snapshotContext.offset,
                                table.id().identifier());

                        if (maxRetries > 0 && isTableSnapshotErrorRetriable(e)) {
                            if ((i + 1) <= maxRetries) {
                                LOGGER.warn("Table {} snapshot failed: {}, attempting to retry ({} of {})",
                                        table.id(), e.getMessage(), i, getTableSnapshotMaxRetries());
                                retrySleeper.pause();
                                continue;
                            }
                        }

                        throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
                    }
                }
            }
            finally {
                offsets.add(offset);
                connectionPool.add(connection);
            }
            return null;
        };
    }

    /**
     * Return the number of times the table's snapshot should be retried.
     *
     * @return the maximum number of snapshot retry attempts.
     */
    private int getTableSnapshotMaxRetries() {
        return connectorConfig.getSnapshotRetryDatabaseErrorsMaxRetries();
    }

    /**
     * Returns whether the specified table snapshot exception is retriable.
     *
     * @param exception the exception that was thrown
     * @return true if the exception should trigger a retry, false if the exception should fail
     */
    protected boolean isTableSnapshotErrorRetriable(SQLException exception) {
        // ORA-01466 - the table's metadata changed during the flashback query.
        // Attempt to recover by having the caller restart the table's snapshot from the beginning.
        return exception.getErrorCode() == 1466;
    }

}
