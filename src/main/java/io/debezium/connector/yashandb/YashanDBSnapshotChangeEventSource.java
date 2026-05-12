/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import com.google.common.collect.Lists;
import com.sics.ystream.result.Position;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.yashandb.snapshot.SnapshotDataSyncTask;
import io.debezium.connector.yashandb.snapshot.SnapshotSQLConstants;
import io.debezium.jdbc.CancellableResultSet;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link StreamingChangeEventSource} for YashanDB.
 *
 * @author Gunnar Morling
 */
public class YashanDBSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<YashanDBPartition, YashanDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDBSnapshotChangeEventSource.class);

    private final YashanDBConnectorConfig connectorConfig;
    private final YashanDBConnection jdbcConnection;
    private final YashanDBDatabaseSchema databaseSchema;
    public final SnapshotProgressListener<YashanDBPartition> snapshotProgressListener;
    private final MainConnectionProvidingConnectionFactory<? extends JdbcConnection> jdbcConnectionFactory;
    private static final int MAX_OBJECT_JOINER_LENGTH = 8000;
    private static final int PARTITION_TABLE_SQL_LEN = 200;
    private static final String YASHAN_DB_CATALOG_NAME = "";
    private static final String AS_OF_SCN = " AS OF SCN ";
    private static String identifierQuote = "\"";
    private static final int SPLIT_SIZE_KB = 64 * 1024;
    private static final int DEFAULT_THREAD_COUNT = 1;
    private static final int MAX_THREAD_COUNT = 64;  //最大64个并发线程处理数据迁移
    private static final String CONDITION_CLAUSE_FORMAT =
            " OR (t.OWNER = %s AND t.SEGMENT_NAME IN (%s))";
    private final String escapeCharacter;
    private final String stringQuote;
    private final List<Character> escapeList;
    private static final int CONDITION_CLAUSE_LENGTH = CONDITION_CLAUSE_FORMAT.length() - 4;
    public final EventDispatcher<YashanDBPartition, TableId> dispatcher;
    protected final Clock clock;

    private static final class TmpRowids {
        private final String partitionName;
        private final String minRowidSql;
        private final String maxRowidSql;

        private TmpRowids(String partitionName, String minRowidSql, String maxRowidSql) {
            this.partitionName = partitionName;
            this.minRowidSql = minRowidSql;
            this.maxRowidSql = maxRowidSql;
        }

        public String partitionName() {
            return partitionName;
        }

        public String minRowidSql() {
            return minRowidSql;
        }

        public String maxRowidSql() {
            return maxRowidSql;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (TmpRowids) obj;
            return Objects.equals(this.partitionName, that.partitionName) &&
                    Objects.equals(this.minRowidSql, that.minRowidSql) &&
                    Objects.equals(this.maxRowidSql, that.maxRowidSql);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionName, minRowidSql, maxRowidSql);
        }

        @Override
        public String toString() {
            return "TmpRowids[" +
                    "partitionName=" + partitionName + ", " +
                    "minRowidSql=" + minRowidSql + ", " +
                    "maxRowidSql=" + maxRowidSql + ']';
        }
    }
    //private ConcurrentMap<TableId, Long> tablesCount;


    public YashanDBSnapshotChangeEventSource(YashanDBConnectorConfig connectorConfig, MainConnectionProvidingConnectionFactory<YashanDBConnection> connectionFactory,
                                             YashanDBDatabaseSchema schema, EventDispatcher<YashanDBPartition, TableId> dispatcher, Clock clock,
                                             SnapshotProgressListener<YashanDBPartition> snapshotProgressListener,
                                             NotificationService<YashanDBPartition, YashanDBOffsetContext> notificationService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
        this.databaseSchema = schema;
        //this.tablesCount =  new ConcurrentHashMap();
        this.snapshotProgressListener = snapshotProgressListener;
        this.jdbcConnectionFactory = connectionFactory;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.escapeCharacter = "'";
        this.stringQuote = "'";
        this.escapeList = List.of('\'');
    }

    @Override
    public SnapshottingTask getSnapshottingTask(YashanDBPartition yashanDBPartition, YashanDBOffsetContext previousOffset) {
        boolean snapshotSchema = true;
        List<String> dataCollectionsToBeSnapshotted = this.connectorConfig.getDataCollectionsToBeSnapshotted();
        Map<String, String> snapshotSelectOverridesByTable = (Map) this.connectorConfig.getSnapshotSelectOverridesByTable().entrySet().stream()
                .collect(Collectors.toMap((e) -> {
                    return ((DataCollectionId) e.getKey()).identifier();
                }, Map.Entry::getValue));
        boolean snapshotData;
        if (YashanDBConnectorConfig.SnapshotMode.ALWAYS == this.connectorConfig.getSnapshotMode()) {
            LOGGER.info("Snapshot mode is set to ALWAYS, not checking exiting offset.");
            snapshotData = this.connectorConfig.getSnapshotMode().includeData();
        } else if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info("The previous offset has been found.");
            snapshotSchema = this.databaseSchema.isStorageInitializationExecuted();
            snapshotData = false;
        } else {
            LOGGER.info("No previous offset has been found.");
            snapshotData = this.connectorConfig.getSnapshotMode().includeData();
        }

        if (snapshotData && snapshotSchema) {
            LOGGER.info("According to the connector configuration both schema and data will be snapshot.");
        } else if (snapshotSchema) {
            LOGGER.info("According to the connector configuration only schema will be snapshot.");
        }

        return new SnapshottingTask(snapshotSchema, snapshotData, dataCollectionsToBeSnapshotted, snapshotSelectOverridesByTable, false);
    }

    @Override
    protected SnapshotContext<YashanDBPartition, YashanDBOffsetContext> prepare(YashanDBPartition partition)
            throws Exception {
        return new YashanDBSnapshotContext(partition, connectorConfig.getDatabaseName());
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> ctx)
            throws Exception {
        return jdbcConnection.getAllTableIds(ctx.catalogName);
        // this very slow approach(commented out), it took 30 minutes on an instance with 600 tables
        // return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[] {"TABLE"} );
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext)
            throws SQLException, InterruptedException {
        if (connectorConfig.getSnapshotLockingMode().usesLocking()) {
            LOGGER.info("Enable snapshot lock captured tables");
            ((YashanDBSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("dbz_schema_snapshot");

            try (Statement statement = jdbcConnection.connection().createStatement()) {
                for (TableId tableId : snapshotContext.capturedTables) {
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while locking table " + tableId);
                    }

                    LOGGER.debug("Locking table {}", tableId);
                    statement.execute("LOCK TABLE " + quote(tableId) + " IN SHARE MODE");
                }
            }
        } else {
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
    }

    @Override
    public void releaseSchemaSnapshotLocks(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext)
            throws SQLException {
        if (connectorConfig.getSnapshotLockingMode().usesLocking()) {
            // release table lock
            jdbcConnection.connection().rollback(((YashanDBSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
        }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> ctx,
                                           YashanDBOffsetContext previousOffset)
            throws Exception {
        // Support the existence of the case when the previous offset.
        // e.g., schema_only_recovery snapshot mode
        if (connectorConfig.getSnapshotMode() != YashanDBConnectorConfig.SnapshotMode.ALWAYS && previousOffset != null) {
            ctx.offset = previousOffset;
            tryStartingSnapshot(ctx);
            return;
        }

        ctx.offset = connectorConfig.getAdapter().determineSnapshotOffset(ctx, connectorConfig, jdbcConnection);
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
                                      YashanDBOffsetContext yashanDBOffsetContext, SnapshottingTask snapshottingTask)
            throws Exception {
        YashanDBSnapshotContext ctx = (YashanDBSnapshotContext) snapshotContext;
        Set<TableId> capturedSchemaTables;
        if (databaseSchema.storeOnlyCapturedTables()) {
            capturedSchemaTables = snapshotContext.capturedTables;
            LOGGER.info("Only captured tables schema should be captured, capturing: {}", capturedSchemaTables);
        } else {
            capturedSchemaTables = snapshotContext.capturedSchemaTables;
            LOGGER.info("All eligible tables schema should be captured, capturing: {}", capturedSchemaTables);
        }

        Set<String> schemas = capturedSchemaTables.stream().map(TableId::schema).collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        // however, for users interested only in captured tables, we need to pass also table filter
        final Tables.TableFilter tableFilter = connectorConfig.storeOnlyCapturedTables() ? connectorConfig.getTableFilters().dataCollectionFilter() : null;
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

        //读取所有表的数据大小
        queryTablesSize(ctx.tables, ctx.tableSplitMap, snapshotContext.offset.getScn().toString());

        //读取分区信息
        queryTablesPartion(ctx.tableSplitMap, snapshotContext.offset.getScn().toString());
    }

    public String quoteIdentifier(final String identifier) {
        return identifierQuote + identifier + identifierQuote;
    }

    public final String getObjectName(final TableId tableInfo) {
        return tableInfo.schema() == null
                ? quoteIdentifier(tableInfo.table())
                : quoteIdentifier(tableInfo.schema()) + "." + quoteIdentifier(tableInfo.table());
    }

    public String getMinRowidSql(
            final SnapshotTableSplitInfo table, final String snapshotOffset) {
        return String.format(
                "SELECT MIN(ROWID) AS %s FROM %s %s",
                YashanDBConnection.DialectQueryField.MIN_ROWID.getName(),
                getObjectName(table.getTableId()),
                AS_OF_SCN + snapshotOffset
        );
    }

    public String getMaxRowidSql(
            final SnapshotTableSplitInfo table, final String snapshotOffset) {
        return String.format(
                "SELECT MAX(ROWID) AS %s FROM %s %s",
                YashanDBConnection.DialectQueryField.MAX_ROWID.getName(),
                getObjectName(table.getTableId()),
                AS_OF_SCN + snapshotOffset
        );
    }

    private String getRowid(
            final Statement statement, final String sql, final YashanDBConnection.DialectQueryField queryField)
            throws SQLException {
        try (final ResultSet rs = statement.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getString(queryField.getName());
            }
        }
        return null;
    }

    public void processSplitColumnQuery(
            String snapshotOffset, final SnapshotTableSplitInfo table) throws SQLException {
        if (Thread.currentThread().isInterrupted()) {
            return;
        }
       /* // 临时修改成两个语句查询，合在一起查询，会导致数据库有不可预期的bug.
        final String minRowidSql = getMinRowidSql(table, snapshotOffset);
        final String maxRowidSql = getMaxRowidSql(table, snapshotOffset);

        //final LazyConnection conn2 = lazyConnection.copy();
        JdbcConnection conn2 = (JdbcConnection) connectionPool.poll();*/
        try {
            /*// 使用 CompletableFuture 并发查询最小值和最大值
            final CompletableFuture<String> minRowidFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return getRowid(
                                            this.readTableStatement(this.jdbcConnection, OptionalLong.empty()),
                                            minRowidSql,
                                            YashanDBConnection.DialectQueryField.MIN_ROWID);
                                } catch (final SQLException e) {
                                    throw new CompletionException(e);
                                }
                            });
            final CompletableFuture<String> maxRowidFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return getRowid(
                                            this.readTableStatement(conn2, OptionalLong.empty()),
                                            maxRowidSql,
                                            YashanDBConnection.DialectQueryField.MAX_ROWID);
                                } catch (final SQLException e) {
                                    throw new CompletionException(e);
                                }
                            });

            // 使用 CompletableFuture.allOf 等待两个查询完成，任何一个失败立即抛出异常
            CompletableFuture.allOf(minRowidFuture, maxRowidFuture).join();

            // 获取查询结果
            final String minRowid = minRowidFuture.getNow(null);
            final String maxRowid = maxRowidFuture.getNow(null);*/

            RowIds rowIdsByView = getRowIdsByView(String.format(SnapshotSQLConstants.QUERY_NO_PARTITION_ROW_ID_SQL,
                    table.getTableId().table(),
                    table.getTableId().schema()
            ));
            if (rowIdsByView != null) {
                String minRowid = rowIdsByView.minRowId;
                String maxRowid = rowIdsByView.maxRowId;
                if ((minRowid == null || maxRowid == null)) { // 对于非空表，rowid不应为空
                    LOGGER.warn("minRowid:{} or maxRowid:{} is null", minRowid, maxRowid);
                    return;
                }
                table.setMinValue(new YaShanRowid(minRowid));
                table.setMaxValue(new YaShanRowid(maxRowid));
                LOGGER.info("query rowid: table: {}, min rowid:{}, max rowid:{}", table.getTableId().table(), minRowid, maxRowid);
            }

        } /* finally {
            if (conn2 != null) {
                connectionPool.add(conn2); //回收
            }
        }*/ catch (SQLException e) {
            LOGGER.error("Error during get minRowid and maxRowid");
            throw e;
        } catch (NumberFormatException e) {
            LOGGER.warn("Rowid set null continue:{}", e.getMessage());
            table.setMinValue(null);
            table.setMaxValue(null);
        }
    }

    public String getDataFileInfoSql() {
        return String.format(
                "select TS# AS %s, RELATIVE_FNO AS %s, BLOCKS AS %s FROM SYS.V_$DATAFILE",
                YashanDBConnection.DialectQueryField.TS_ID.getName(),
                YashanDBConnection.DialectQueryField.DATAFILE_NO.getName(),
                YashanDBConnection.DialectQueryField.BLOCKS.getName()
        );
    }

    public Map<String, Integer> queryDatafileInfo() throws Exception {
        HashMap<String, Integer> datafileMap = new HashMap<>();
        String sql = getDataFileInfoSql();
        this.jdbcConnection.queryDatafileInfo(sql, datafileMap);
        return datafileMap;
    }

    public String getPartitionTableMinRowidSql(
            final TableId tableInfo,
            final String partitionName,
            String snapshotOffset) {
        return String.format(
                "SELECT MIN(ROWID) AS %s FROM %s partition (%s) %s",
                YashanDBConnection.DialectQueryField.MIN_ROWID.getName(),
                getObjectName(tableInfo),
                quoteIdentifier(partitionName),
                AS_OF_SCN + snapshotOffset
        );
    }

    public String getPartitionTableMaxRowidSql(
            final TableId tableInfo,
            final String partitionName,
            String snapshotOffset) {
        return String.format(
                "SELECT MAX(ROWID) AS %s FROM %s partition (%s) %s",
                YashanDBConnection.DialectQueryField.MAX_ROWID.getName(),
                getObjectName(tableInfo),
                quoteIdentifier(partitionName),
                AS_OF_SCN + snapshotOffset
        );
    }


    private void queryRowid(
            final TableId objectInfo,
            final TmpRowids rowid,
            final ArrayList<YaShanPartitionSplitRecord> res) {
//        final JdbcConnection conn2 = connectionPool.poll();
        try {
/*            // 使用 CompletableFuture 并发查询最小值和最大值
            final CompletableFuture<String> minRowidFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return getRowid(
                                            this.readTableStatement(this.jdbcConnection, OptionalLong.empty()),
                                            rowid.minRowidSql(),
                                            YashanDBConnection.DialectQueryField.MIN_ROWID);
                                } catch (final SQLException e) {
                                    throw new CompletionException(e);
                                }
                            });
            final CompletableFuture<String> maxRowidFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return getRowid(
                                            this.readTableStatement(conn2, OptionalLong.empty()),
                                            rowid.maxRowidSql(),
                                            YashanDBConnection.DialectQueryField.MAX_ROWID);
                                } catch (final SQLException e) {
                                    throw new CompletionException(e);
                                }
                            });

            // 使用 CompletableFuture.allOf 等待两个查询完成，任何一个失败立即抛出异常
            CompletableFuture.allOf(minRowidFuture, maxRowidFuture).join();

            // 获取查询结果
            final String minRowid = minRowidFuture.getNow(null);
            final String maxRowid = maxRowidFuture.getNow(null);*/

            RowIds rowIdsByView = getRowIdsByView(String.format(SnapshotSQLConstants.QUERY_PARTITION_ROW_ID_SQL,
                    objectInfo.schema(),
                    objectInfo.table(),
                    rowid.partitionName
            ));

            if (rowIdsByView != null) {
                String minRowid = rowIdsByView.minRowId;
                String maxRowid = rowIdsByView.maxRowId;

                if ((minRowid == null || maxRowid == null)) {
                    // 查询数据量时，虽然有很大数据量，但是该分区可能为空。
                    res.add(new YaShanPartitionSplitRecord(objectInfo, rowid.partitionName(), null, null));
                } else {
                    res.add(
                            new YaShanPartitionSplitRecord(
                                    objectInfo, rowid.partitionName(), new YaShanRowid(minRowid), new YaShanRowid(maxRowid)));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query partition rowid for " + objectInfo, e);
        } catch (NumberFormatException e) {
            LOGGER.warn("Rowid set null continue:{}", e.getMessage());
            res.add(new YaShanPartitionSplitRecord(objectInfo, rowid.partitionName(), null, null));
        } /*finally {
            if (conn2 != null) {
                connectionPool.add(conn2); //回收
            }
        }*/
    }

    public List<YaShanPartitionSplitRecord> processPartitionSplitColumnQuery(
            final TableId tableInfo,
            final YaShanDBPartitionInfo partitionInfo,
            final String snapshotOffset) {
        // 临时修改成两个语句查询，合在一起查询，会导致数据库有不可预期的bug.
        final ArrayList<TmpRowids> rowids = new ArrayList<>();
        final ArrayList<YaShanPartitionSplitRecord> res = new ArrayList<>();
        for (YaShanDBPartitionInfo.SubPartitionInfo subPartitionInfo : partitionInfo.getSubPartitionInfo()) {
            if (subPartitionInfo.size() < SPLIT_SIZE_KB) {
                LOGGER.info(
                        "Partition Table: {}, partitionName: {}, size: {}, not need split rowids.",
                        tableInfo,
                        subPartitionInfo.partitionName(),
                        subPartitionInfo.size());
                res.add(new YaShanPartitionSplitRecord(tableInfo, subPartitionInfo.partitionName(), null, null));
                continue;
            }
            final String minRowidSql =
                    getPartitionTableMinRowidSql(
                            tableInfo, subPartitionInfo.partitionName(), snapshotOffset);
            final String maxRowidSql =
                    getPartitionTableMaxRowidSql(
                            tableInfo, subPartitionInfo.partitionName(), snapshotOffset);
            rowids.add(new TmpRowids(subPartitionInfo.partitionName(), minRowidSql, maxRowidSql));
        }

        for (final TmpRowids rowid : rowids) {
            queryRowid(tableInfo, rowid, res);
        }

        LOGGER.info("query partition table rowid: table: {}, rowid: {}.", tableInfo.toString(), res);
        return res;
    }

    public String getSnapshotQuerySql(
            final TableId table,
            final List<String> columnsName,
            final String snapshotOffset) {

        final String sql = "SELECT %s FROM %s" + AS_OF_SCN + snapshotOffset;
        return String.format(
                sql,
                columnsName.stream().map(this::quoteIdentifier).collect(Collectors.joining(",")),
                getObjectName(table));
    }


    public String getSnapshotPartitionTableQuerySql(
            final TableId table,
            final String partitionName,
            final List<String> columnNames,
            final String snapshotOffset) {
        final String sql =
                String.format(
                        "SELECT %s FROM %s",
                        columnNames.stream().map(this::quoteIdentifier).collect(Collectors.joining(",")),
                        getObjectName(table));
        final String ofScn = AS_OF_SCN + snapshotOffset;
        return String.format(
                "%s PARTITION (%s) %s",
                sql,
                quoteIdentifier(partitionName),
                ofScn
        );
    }

    public String getSnapshotSplitSql(
            final TableId table,
            final List<String> columnsName,
            final String snapshotOffset,
            final String minRowid,
            final String maxRowid,
            final Column splitColumn) {
        String whereClause = " WHERE 1=1";
        if (minRowid != null) {
            whereClause += " AND ROWID >= " + quoteStringClause(minRowid);
        }
        if (maxRowid != null) {
            whereClause += " AND ROWID < " + quoteStringClause(maxRowid);
        }
        return getSnapshotQuerySql(table, columnsName, snapshotOffset) + whereClause;
    }

    public String getPartitionSnapshotSplitSql(
            final TableId table,
            final List<String> columnsName,
            final String snapshotOffset,
            final String minRowid,
            final String maxRowid,
            final Column splitColumn,
            final String partitionName) {
        String whereClause = " WHERE 1=1";
        if (minRowid != null) {
            whereClause += " AND ROWID >= " + quoteStringClause(minRowid);
        }
        if (maxRowid != null) {
            whereClause += " AND ROWID < " + quoteStringClause(maxRowid);
        }
        return getSnapshotPartitionTableQuerySql(table, partitionName, columnsName, snapshotOffset)
                + whereClause;
    }

    private String getFirstSplitSql(
            final SnapshotTableSplitInfo table,
            final String snapshotOffset,
            final String data,
            final String partitionName) {
        return partitionName == null
                ? getSnapshotSplitSql(
                table.getTableId(), table.getColumnNames(), snapshotOffset, null, data, null)
                : getPartitionSnapshotSplitSql(
                table.getTableId(),
                table.getColumnNames(),
                snapshotOffset,
                null,
                data,
                null,
                partitionName);
    }

    private String getLastSplitSql(
            final SnapshotTableSplitInfo table,
            final String snapshotOffset,
            final String data,
            final String partitionName) {
        return partitionName == null
                ? getSnapshotSplitSql(
                table.getTableId(), table.getColumnNames(), snapshotOffset, data, null, null)
                : getPartitionSnapshotSplitSql(
                table.getTableId(),
                table.getColumnNames(),
                snapshotOffset,
                data,
                null,
                null,
                partitionName);
    }

    public void splitTableSqlNoRowid(final SnapshotTableSplitInfo tableInfor,
                                     String partitionName,
                                     String snapshotOffset,
                                     ArrayList<String> list) {

        if (partitionName == null) {
            final String sql =
                    getSnapshotQuerySql(
                            tableInfor.getTableId(),
                            tableInfor.getColumnNames(),
                            snapshotOffset);
            LOGGER.warn("partition table:{} ,but the partition name is null", tableInfor.getTableId());
            list.add(sql);
        } else {
            final String sql =
                    getSnapshotPartitionTableQuerySql(
                            tableInfor.getTableId(),
                            partitionName,
                            tableInfor.getColumnNames(),
                            snapshotOffset);
            list.add(sql);
        }

    }

    public void splitTableSql(final SnapshotTableSplitInfo tableInfor,
                              String partitionName,
                              List<YaShanRowid> splitData,
                              String snapshotOffset,
                              ArrayList<String> list) {

        if (tableInfor.isEmptyTable()) {
            return;
        } else if (splitData.isEmpty()) {
            if (partitionName == null) {
                final String sql =
                        getSnapshotQuerySql(
                                tableInfor.getTableId(),
                                tableInfor.getColumnNames(),
                                snapshotOffset);
                list.add(sql);
            } else {
                final String sql =
                        getSnapshotPartitionTableQuerySql(
                                tableInfor.getTableId(),
                                partitionName,
                                tableInfor.getColumnNames(),
                                snapshotOffset);
                list.add(sql);
            }
        } else {
            list.add(
                    getFirstSplitSql(
                            tableInfor,
                            snapshotOffset,
                            splitData.get(0).getRowid(),
                            partitionName));
            list.add(
                    getLastSplitSql(
                            tableInfor,
                            snapshotOffset,
                            splitData.get(splitData.size() - 1).getRowid(),
                            partitionName));
            for (int i = 0; i < splitData.size() - 1; i++) {
                list.add(
                        partitionName == null
                                ? getSnapshotSplitSql(
                                tableInfor.getTableId(),
                                tableInfor.getColumnNames(),
                                snapshotOffset,
                                splitData.get(i).getRowid(),
                                splitData.get(i + 1).getRowid(),
                                null)
                                : getPartitionSnapshotSplitSql(
                                tableInfor.getTableId(),
                                tableInfor.getColumnNames(),
                                snapshotOffset,
                                splitData.get(i).getRowid(),
                                splitData.get(i + 1).getRowid(),
                                null,
                                partitionName));
            }
        }
    }


    public void partitionSplitTable(
            String snapshotOffset,
            final SnapshotTableSplitInfo table,
            final int parallelism,
            final Map<String, Integer> datafileInfo,
            Map<TableId, ArrayList<String>> queryTablesSql)
            throws Exception {
        //获取每个分区的minRowid和MaxRowid
        final List<YaShanPartitionSplitRecord> rowids =
                processPartitionSplitColumnQuery(
                        table.getTableId(),
                        table.getPartitionInfo(),
                        snapshotOffset);
        int partCount = 0;
        for (final YaShanPartitionSplitRecord rowid : rowids) {
            if (rowid.minrowid() == null || rowid.maxrowid() == null) {
                //此种情况不代表没有数据，可能是数据量比较小，无需再划分,通过分区名直接获取数据
                splitTableSqlNoRowid(table, rowid.partitionName(), snapshotOffset, queryTablesSql.get(table.getTableId()));
                partCount++;
                //LOGGER.warn("The min rowid or max rowid for partitioned tables are null,Table:{} data will not be migrated", table.getTableId());
                continue;
            }
            //对分区再根据rowid进行划分
            List<YaShanRowid> splitRowids =
                    YaShanRowidSplitUtil.getYashanDBSplitRowid(
                            rowid.minrowid(), rowid.maxrowid(), parallelism, datafileInfo);

            LOGGER.info(
                    "Partition Table: {}, partitionName: {}, split rowids : {}",
                    table.getTableId().toString(),
                    rowid.partitionName(),
                    splitRowids.stream().map(YaShanRowid::getRowid).collect(Collectors.toList()));
            partCount += splitRowids.size() + 1;
            //每
            splitTableSql(table, rowid.partitionName(), splitRowids, snapshotOffset, queryTablesSql.get(table.getTableId()));
        }
        table.setPartCount(partCount);
        table.setInitialized(true);
    }

    public void notPartitionSplitTable(String snapshotOffset,
                                       final SnapshotTableSplitInfo table,
                                       final int parallelism,
                                       final Map<String, Integer> datafileInfo,
                                       Map<TableId, ArrayList<String>> queryTablesSql) throws Exception {
        processSplitColumnQuery(snapshotOffset, table);


        if (null == table.getMinValue() || null == table.getMaxValue()) {
            //空表不进行处理
            LOGGER.warn("The min rowid and max rowid for non-partitioned tables are null,Table:{} data will not be migrated", table.getTableId());
            return;
        }
        final int expectSplitCount =
                (int)
                        Math.ceil((float) table.getKiloByteSize() / SPLIT_SIZE_KB);
        final List<YaShanRowid> splitRowids =
                YaShanRowidSplitUtil.getYashanDBSplitRowid(
                        (YaShanRowid) table.getMinValue(),
                        (YaShanRowid) table.getMaxValue(),
                        Math.min(expectSplitCount, parallelism),
                        datafileInfo);
        if (splitRowids.isEmpty()) {
            if (parallelism > 1) {
                LOGGER.warn(
                        "Split table {} failed, we'll migrate it in single thread.",
                        table.getTableId().toString());
            }
            table.setPartCount(1);
            table.setInitialized(true);
        } else {
            table.setPartCount(splitRowids.size() + 1);
            table.setInitialized(true);
        }
        splitTableSql(table, null, splitRowids, snapshotOffset, queryTablesSql.get(table.getTableId()));
    }

    public String getOneRecordSql(
            final TableId table, String snapshotOffset) {
        return "SELECT 1 FROM "
                + getObjectName(table)
                + AS_OF_SCN + snapshotOffset
                + " WHERE ROWNUM = 1";
    }

    public boolean tableIsEmptyAsOfScn(TableId tableId, String snapshotOffset) throws SQLException {

        //String sql = String.format("SELECT 1 FROM %s AS OF SCN %s  WHERE ROWNUM = 1", tableName, scn);
        String sql = getOneRecordSql(tableId, snapshotOffset);
        return this.jdbcConnection.tableIsEmptyAsOfScn(sql);
    }

    protected void queryEmptyTableAsOfScn(
            Tables tablesInfor,
            Set<TableId> tableIds,
            ConcurrentMap<TableId, SnapshotTableSplitInfo> map,
            String snapshotOffset)
            throws Exception {
        for (TableId table : tableIds) {
            SnapshotTableSplitInfo tableInfo = new SnapshotTableSplitInfo(table);
            //初始化tableInfo表的列名字信息，后续生成对应的查询sql语句需要使用到
            for (Column column : tablesInfor.forTable(table).columns()) {
                tableInfo.getColumnNames().add(column.name());
            }
            map.put(table, tableInfo);
            // 表太多时，跳过空表查询
            if (tableIds.size() > 5000) {
                continue;
            }
            //boolean tableEmpty =this.jdbcConnection.tableIsEmptyAsOfScn(table.toString(),snapshotOffset);
            boolean tableEmpty = tableIsEmptyAsOfScn(table, snapshotOffset);
            map.get(table).setEmptyTable(tableEmpty);
        }
    }

    private final String quoteStringClause(final String clause) {
        StringBuilder clauseBuilder = new StringBuilder(stringQuote);
        int len = clause.length();
        for (int i = 0; i < len; i++) {
            char c = clause.charAt(i);
            if (escapeList.contains(c)) {
                clauseBuilder.append(escapeCharacter);
            }
            clauseBuilder.append(c);
        }
        return clauseBuilder.append(stringQuote).toString();
    }

    private String whereConditionJoiner(final Map<String, List<String>> schemaTables) {
        final StringBuilder whereCondition = new StringBuilder();
        schemaTables.forEach(
                (key, value) ->
                        whereCondition.append(
                                String.format(
                                        CONDITION_CLAUSE_FORMAT,
                                        key,
                                        String.join(",", value)
                                )
                        )
        );
        return whereCondition.toString();
    }

    // 生成一组where条件
    protected List<String> processWhereCondition(
            final Map<String, List<SnapshotTableSplitInfo>> tableMap, final int maxLength) {
        final List<String> conditionSqlList = new ArrayList<>();
        final AtomicInteger availableLength = new AtomicInteger(maxLength);
        final HashMap<String, List<String>> schemaTableMap = new HashMap<>();
        for (final Map.Entry<String, List<SnapshotTableSplitInfo>> entry : tableMap.entrySet()) {

            final List<SnapshotTableSplitInfo> notEmptyTables =
                    entry.getValue().stream()
                            .filter(t -> !t.isEmptyTable())
                            .collect(Collectors.toList());
            if (null == notEmptyTables || notEmptyTables.isEmpty()) {
                continue;
            }
            availableLength.set(availableLength.get() - CONDITION_CLAUSE_LENGTH);
            availableLength.getAndAdd(1); // 补偿table的第一个逗号

            final String schema = quoteStringClause(entry.getKey());
            final ArrayList<String> tables = new ArrayList<>();
            schemaTableMap.put(schema, tables);
            notEmptyTables.forEach(
                    t -> {
                        final String tableName = quoteStringClause(t.getTableId().table());
                        availableLength.set(
                                availableLength.get() - schema.getBytes().length - tableName.getBytes().length - 1);
                        // 剩余容量能够容纳此表
                        if (availableLength.get() > 0) {
                            tables.add(tableName);
                        } else { // 剩余容量无法容纳此表，则此where子句拼接完成，存入conditionList，availableLength重置
                            if (schemaTableMap.get(schema).isEmpty()) {
                                schemaTableMap.remove(schema);
                            }
                            if (!schemaTableMap.isEmpty()) {
                                conditionSqlList.add(whereConditionJoiner(schemaTableMap));
                            }
                            schemaTableMap.clear();
                            tables.clear();
                            tables.add(tableName);
                            schemaTableMap.put(schema, tables);
                            // 创建新的 OR (OWNER = %s AND SEGMENT_NAME IN (%s)) 条件子句
                            availableLength.set(
                                    maxLength
                                            - CONDITION_CLAUSE_LENGTH
                                            - schema.getBytes().length
                                            - tableName.getBytes().length);
                        }
                    });
        }
        if (!schemaTableMap.isEmpty()) {
            conditionSqlList.add(whereConditionJoiner(schemaTableMap));
        }
        schemaTableMap.clear();
        return conditionSqlList;
    }

    protected List<String> getTableSizeAsOfScnSqls(
            Map<String, List<SnapshotTableSplitInfo>> tablesBySchema,
            final String snapshotOffset) {

        final String sql = String.format(
                "SELECT\n" +
                        "  OWNER AS %s,\n" +
                        "  SEGMENT_NAME AS %s,\n" +
                        "  SUM(NVL(BYTES, 0)) / 1024 AS %s\n" +
                        "FROM SYS.DBA_SEGMENTS %%s t\n" +
                        "  WHERE FALSE\n" +
                        "    %%s\n" +
                        "  GROUP BY t.OWNER, t.SEGMENT_NAME",
                YashanDBConnection.DialectQueryField.SCHEMA_NAME.getName(),
                YashanDBConnection.DialectQueryField.TABLE_NAME.getName(),
                YashanDBConnection.DialectQueryField.DATA_SIZE.getName()
        );


        final int scnLength = 18;
        final int availableLength = MAX_OBJECT_JOINER_LENGTH - sql.getBytes().length + 4 - scnLength;

        final List<String> sqlList = new ArrayList<>();
        for (final String whereCondition : processWhereCondition(tablesBySchema, availableLength)) {
            sqlList.add(String.format(sql, AS_OF_SCN + snapshotOffset, whereCondition));
        }
        return sqlList;
    }

    public List<String> getTableLobSizeSqls(
            final Map<String, List<SnapshotTableSplitInfo>> tableMap,
            final String snapshotOffset) {

        final String sql = String.format(
                "SELECT\n" +
                        "  t.OWNER AS %s,\n" +
                        "  t.TABLE_NAME AS %s,\n" +
                        "  SUM(s1.BYTES)/1024 AS %s\n" +
                        "FROM SYS.DBA_LOBS %%s t\n" +
                        "JOIN SYS.DBA_SEGMENTS %%s s1\n" +
                        "  ON t.OWNER = s1.OWNER\n" +
                        "    AND t.SEGMENT_NAME = s1.SEGMENT_NAME\n" +
                        "WHERE FALSE\n" +
                        "  %%s\n" +
                        "GROUP BY t.OWNER, t.TABLE_NAME",
                YashanDBConnection.DialectQueryField.SCHEMA_NAME.getName(),
                YashanDBConnection.DialectQueryField.TABLE_NAME.getName(),
                YashanDBConnection.DialectQueryField.DATA_SIZE.getName()
        );


        final int scnLength = 18;
        final int availableLength = MAX_OBJECT_JOINER_LENGTH - sql.getBytes().length + 6 - scnLength;

        final List<String> sqlList = new ArrayList<>();
        for (final String whereCondition : processWhereCondition(tableMap, availableLength)) {
            sqlList.add(
                    String.format(
                            sql, AS_OF_SCN + snapshotOffset, AS_OF_SCN + snapshotOffset, whereCondition));
        }
        return sqlList;
    }

    public void queryTablesSize(
            Tables tables,
            ConcurrentMap<TableId, SnapshotTableSplitInfo> tableSplitMap,
            String snapshotOffset) throws Exception {
        int count = 0;
        // 获取所有表ID
        Set<TableId> tableIds = tables.tableIds();


        LOGGER.info("Begin table size query");
        if (tableIds.isEmpty()) {
            LOGGER.info("Finish table size query ,the tables is empty");
            return;
        }
        try {
            //查询表是否为空表
            queryEmptyTableAsOfScn(tables, tableIds, tableSplitMap, snapshotOffset);

            // 按schema分组表
            Map<String, List<SnapshotTableSplitInfo>> tablesBySchema = tableSplitMap.entrySet().stream()
                    .collect(Collectors.groupingBy(
                            entry -> entry.getKey().schema(), // 按 schemaName 分组
                            Collectors.mapping(
                                    Map.Entry::getValue, // 提取 SnapshotTableSplitInfo
                                    Collectors.toList()  // 收集到 List 中
                            )
                    ));

            //查询非空表的数据量大小
            for (String sql : getTableSizeAsOfScnSqls(tablesBySchema, snapshotOffset)) {
                count++;
                this.jdbcConnection.processTableSizeQuery(sql, tableSplitMap);
            }
            //再获取Lob字段的数据量大小
            for (String sql : getTableLobSizeSqls(tablesBySchema, snapshotOffset)) {
                count++;
                this.jdbcConnection.processTableSizeQuery(sql, tableSplitMap);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOGGER.info("Finish table size query,table count :{} ", count);
    }


    public String queryTableIsPartitionSql(
            final String schema,
            final List<String> tables,
            final String snapshotOffset) {

        return String.format(
                "SELECT TABLE_NAME as TABLE_NAME,\n" +
                        "       CASE WHEN PARTITIONED = 'Y' THEN 'TRUE' ELSE 'FALSE' END as HAVE_PARTITION\n" +
                        "FROM DBA_TABLES %s\n" +
                        "WHERE OWNER = %s\n" +
                        "  AND TABLE_NAME in (%s)",
                AS_OF_SCN + snapshotOffset,
                quoteStringClause(schema),
                String.join(",", tables.stream().map(this::quoteStringClause).collect(Collectors.toList()))
        );
    }

    public String queryPartitionInforSql(
            final String schema,
            final List<String> tables,
            final String snapshotOffset) {

        return String.format(
                "SELECT C.TABLE_OWNER,\n" +
                        "       C.TABLE_NAME,\n" +
                        "       C.PARTITION_NAME,\n" +
                        "       A.BYTES\n" +
                        "FROM DBA_TAB_PARTITIONS %3$s C\n" +
                        "JOIN (SELECT OWNER, SEGMENT_NAME, PARTITION_NAME, SUM(NVL(BYTES, 0)) / 1024 AS BYTES\n" +
                        "      FROM DBA_SEGMENTS %3$s\n" +
                        "      WHERE SEGMENT_TYPE IN ('TABLE PARTITION')\n" +
                        "      GROUP BY SEGMENT_NAME, PARTITION_NAME, OWNER) A\n" +
                        "  ON C.TABLE_OWNER = A.OWNER\n" +
                        "     AND C.TABLE_NAME = A.SEGMENT_NAME\n" +
                        "     AND C.PARTITION_NAME = A.PARTITION_NAME\n" +
                        "WHERE C.TABLE_OWNER = %1$s\n" +
                        "  AND C.TABLE_NAME IN (%2$s)\n" +
                        "UNION ALL\n" +
                        "SELECT C.TABLE_OWNER,\n" +
                        "       C.TABLE_NAME,\n" +
                        "       C.PARTITION_NAME,\n" +
                        "       SUM(A.BYTES) AS BYTES\n" +
                        "FROM DBA_TAB_SUBPARTITIONS %3$s C\n" +
                        "JOIN (SELECT OWNER, SEGMENT_NAME, PARTITION_NAME, SUM(NVL(BYTES, 0)) / 1024 AS BYTES\n" +
                        "      FROM DBA_SEGMENTS %3$s\n" +
                        "      WHERE SEGMENT_TYPE IN ('TABLE SUBPARTITION')\n" +
                        "      GROUP BY SEGMENT_NAME, PARTITION_NAME, OWNER) A\n" +
                        "  ON C.TABLE_OWNER = A.OWNER\n" +
                        "     AND C.TABLE_NAME = A.SEGMENT_NAME\n" +
                        "     AND C.SUBPARTITION_NAME = A.PARTITION_NAME\n" +
                        "WHERE C.TABLE_OWNER = %1$s\n" +
                        "  AND C.TABLE_NAME IN (%2$s)\n" +
                        "GROUP BY C.TABLE_OWNER, C.TABLE_NAME, C.PARTITION_NAME",
                quoteStringClause(schema),
                String.join(",", tables.stream().map(this::quoteStringClause).collect(Collectors.toList())),
                AS_OF_SCN + snapshotOffset);
    }

    public void queryTablesIsPartitioned(
            String schema,
            List<String> tables,
            String snapshotOffset,
            ConcurrentMap<TableId, YaShanDBPartitionInfo> tablePartitionMap)
            throws Exception {
        for (final List<String> infos : Lists.partition(tables, PARTITION_TABLE_SQL_LEN)) {
            final String sql = queryTableIsPartitionSql(schema, infos, snapshotOffset);
            this.jdbcConnection.batchCheckTablesArePartitioned(schema, sql, tablePartitionMap);
        }
    }

    public void queryTablesPartitionInfor(
            String schema,
            List<String> tables,
            String snapshotOffset,
            ArrayList<YaShanDBPartitionInfo.SubPartitionInfo> tablePartitionInfor)
            throws Exception {

        for (final List<String> infos : Lists.partition(tables, PARTITION_TABLE_SQL_LEN)) {
            final String sql = queryPartitionInforSql(schema, infos, snapshotOffset);
            this.jdbcConnection.queryTablesPartitionInfor(sql, tablePartitionInfor);
        }
    }

    public void queryTablesPartion(
            ConcurrentMap<TableId, SnapshotTableSplitInfo> tableSplitMap,
            String snapshotOffset) throws Exception {

        // 按schema分组表
        Map<String, List<SnapshotTableSplitInfo>> tablesBySchema = tableSplitMap.entrySet().stream()
                .collect(Collectors.groupingBy(
                        entry -> entry.getKey().schema(), // 按 schemaName 分组
                        Collectors.mapping(
                                Map.Entry::getValue, // 提取 SnapshotTableSplitInfo
                                Collectors.toList()  // 收集到 List 中
                        )
                ));
        //查询表是否被分区了，一个schma获取一次
        ConcurrentMap<TableId, YaShanDBPartitionInfo> tablePartitionMap = new ConcurrentHashMap();
        for (Map.Entry<String, List<SnapshotTableSplitInfo>> entry : tablesBySchema.entrySet()) {
            String schema = entry.getKey(); // 获取键
            List<SnapshotTableSplitInfo> tableList = entry.getValue(); // 获取值（列表）

            //queryTablesIsPartitioned(schema, tableList.stream().map(o -> o.getTableId().table()).toList(),snapshotOffset,tablePartitionMap);
            queryTablesIsPartitioned(
                    schema,
                    tableList.stream()
                            .map(o -> o.getTableId().table())
                            .collect(Collectors.toList()),
                    snapshotOffset,
                    tablePartitionMap
            );
        }
        if (null != tablePartitionMap) {
            for (Map.Entry<TableId, SnapshotTableSplitInfo> entry : tableSplitMap.entrySet()) {
                TableId tableId = entry.getKey(); // 获取键（表ID）
                SnapshotTableSplitInfo splitInfo = entry.getValue(); // 获取值（分区信息）
                YaShanDBPartitionInfo newPartitionInfo = tablePartitionMap.get(tableId);
                if (null != newPartitionInfo) {
                    splitInfo.getPartitionInfo().setIsPartition(newPartitionInfo.getIsPartition());
                }
            }
        }
        //获取表的分区信息，一个schma获取一次
        for (Map.Entry<String, List<SnapshotTableSplitInfo>> entry : tablesBySchema.entrySet()) {
            String schema = entry.getKey();
            ArrayList<YaShanDBPartitionInfo.SubPartitionInfo> tablePartitionInfor = new ArrayList<>();
            List<SnapshotTableSplitInfo> tableList = entry.getValue(); // 获取值（列表）
            //queryTablesPartitionInfor(schema, tableList.stream().map(o -> o.getTableId().table()).toList(),snapshotOffset,tablePartitionInfor);
            queryTablesPartitionInfor(
                    schema,
                    tableList.stream()
                            .map(o -> o.getTableId().table())
                            .collect(Collectors.toList()),  // 修复这里
                    snapshotOffset,
                    tablePartitionInfor
            );
            for (YaShanDBPartitionInfo.SubPartitionInfo subPartition : tablePartitionInfor) {
                SnapshotTableSplitInfo splitInfo = tableSplitMap.get(new TableId(YASHAN_DB_CATALOG_NAME, schema, subPartition.tableName()));
                splitInfo.getPartitionInfo().getSubPartitionInfo().add(subPartition);
            }
        }
    }


    @Override
    protected String enhanceOverriddenSelect(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
                                             String overriddenSelect, TableId tableId) {
        String snapshotOffset = (String) snapshotContext.offset.getOffset().get(SourceInfo.SCN_KEY);
        String token = connectorConfig.getTokenToReplaceInSnapshotPredicate();
        if (token != null) {
            return overriddenSelect.replaceAll(token, " AS OF SCN " + snapshotOffset);
        }
        return overriddenSelect;
    }

    @Override
    protected Collection<TableId> getTablesForSchemaChange(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext) {
        return snapshotContext.capturedSchemaTables;
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
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
    public Instant getSnapshotSourceTimestamp(JdbcConnection jdbcConnection, YashanDBOffsetContext offset, TableId tableId) {
        try {
            Optional<Instant> snapshotTs = ((YashanDBConnection) jdbcConnection).getScnToTimestamp(offset.getScn());
            if (snapshotTs.isEmpty()) {
                throw new ConnectException("Failed reading SCN timestamp from source database");
            }

            return snapshotTs.get();
        } catch (SQLException e) {
            throw new ConnectException("Failed reading SCN timestamp from source database", e);
        }
    }

    /**
     * Generate a valid Oracle query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        final YashanDBOffsetContext offset = snapshotContext.offset;
        final String snapshotOffset = offset.getScn().toString();
        String snapshotSelectColumns = columns.stream()
                .collect(Collectors.joining(", "));
        assert snapshotOffset != null;
        return Optional.of(String.format("SELECT %s FROM %s AS OF SCN %s", snapshotSelectColumns, quote(tableId), snapshotOffset));
    }

    @Override
    protected List<Pattern> getSignalDataCollectionPattern(String signalingDataCollection) {
        // Oracle expects this value to be supplied using "<database>.<schema>.<table>"; however the
        // TableIdMapper used by the connector uses only "<schema>.<table>". This primarily targets
        // a fix for this specific use case as a much larger refactor is likely necessary long term.
        final TableId tableId = TableId.parse(signalingDataCollection);
        return Strings.listOfRegex(tableId.schema() + "." + tableId.table(), Pattern.CASE_INSENSITIVE);
    }

    @Override
    public void close() {
    }

    private String quote(TableId tableId) {
        return new TableId(null, tableId.schema(), tableId.table()).toDoubleQuotedString();
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    public static class YashanDBSnapshotContext extends RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> {

        private Savepoint preSchemaSnapshotSavepoint;
        public ConcurrentMap<TableId, SnapshotTableSplitInfo> tableSplitMap;

        YashanDBSnapshotContext(YashanDBPartition partition, String catalogName) throws SQLException {
            super(partition, catalogName);
            this.tableSplitMap = new ConcurrentHashMap();
        }
    }

    @Override
    public YashanDBOffsetContext copyOffset(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext) {
        return load(snapshotContext.offset.getOffset());
    }

    public YashanDBOffsetContext load(Map<String, ?> offset) {
        boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
        boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(YashanDBOffsetContext.SNAPSHOT_COMPLETED_KEY));
        boolean isCreateServer = Boolean.TRUE.equals(offset.get(YashanDBOffsetContext.YSTREAM_SERVER_CREATE));
        Scn scn = YashanDBOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        CommitScn commitScn = CommitScn.load(offset);
        Map<String, Scn> snapshotPendingTransactions = YashanDBOffsetContext.loadSnapshotPendingTransactions(offset);
        Scn snapshotScn = YashanDBOffsetContext.loadSnapshotScn(offset);
        Scn ystreamStartScn = YashanDBOffsetContext.loadYstreamStartScn(offset);
        Position recoverPosition = YashanDBOffsetContext.loadRecoverPosition(offset);
        return new YashanDBOffsetContext(connectorConfig, scn, commitScn, snapshotScn, ystreamStartScn, recoverPosition, snapshotPendingTransactions, snapshot,
                snapshotCompleted,
                TransactionContext.load(offset),
                SignalBasedIncrementalSnapshotContext.load(offset), isCreateServer);
    }

    private Stream<TableId> YaShanToTableIds(Set<TableId> tableIds, Pattern pattern) {
        return tableIds.stream().filter((tid) -> pattern.asMatchPredicate().test(this.connectorConfig.getTableIdMapper().toString(tid))).sorted();
    }

    private Set<TableId> YaShanAddSignalingCollectionAndSort(Set<TableId> capturedTables) {
        String tableIncludeList = this.connectorConfig.tableIncludeList();
        String signalingDataCollection = this.connectorConfig.getSignalingDataCollectionId();
        List<Pattern> captureTablePatterns = new ArrayList();
        if (!Strings.isNullOrBlank(tableIncludeList)) {
            captureTablePatterns.addAll(Strings.listOfRegex(tableIncludeList, 2));
        } else {
            captureTablePatterns.add(MATCH_ALL_PATTERN);
        }

        if (!Strings.isNullOrBlank(signalingDataCollection)) {
            captureTablePatterns.addAll(this.getSignalDataCollectionPattern(signalingDataCollection));
        }

        return (Set) captureTablePatterns.stream().flatMap((pattern) -> this.YaShanToTableIds(capturedTables, pattern)).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private void YaShanDetermineCapturedTables(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> ctx,
                                               Set<Pattern> dataCollectionsToBeSnapshotted, SnapshottingTask snapshottingTask) throws Exception {
        Set<TableId> allTableIds = this.getAllTableIds(ctx);
        Set<TableId> snapshottedTableIds = (Set) this.determineDataCollectionsToBeSnapshotted(allTableIds, dataCollectionsToBeSnapshotted).collect(Collectors.toSet());
        Set<TableId> capturedTables = new HashSet();
        Set<TableId> capturedSchemaTables = new HashSet();

        for (TableId tableId : allTableIds) {
            if (this.connectorConfig.getTableFilters().eligibleForSchemaDataCollectionFilter().isIncluded(tableId) && !snapshottingTask.isBlocking()) {
                LOGGER.info("Adding table {} to the list of capture schema tables", tableId);
                capturedSchemaTables.add(tableId);
            }
        }

        for (TableId tableId : snapshottedTableIds) {
            if (this.connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                LOGGER.info("Adding table {} to the list of captured tables for which the data will be snapshotted", tableId);
                capturedTables.add(tableId);
            } else {
                LOGGER.trace("Ignoring table {} for data snapshotting as it's not included in the filter configuration", tableId);
            }
        }

        ctx.capturedTables = this.YaShanAddSignalingCollectionAndSort(capturedTables);
        ctx.capturedSchemaTables = snapshottingTask.isBlocking() ? ctx.capturedTables : (Set) capturedSchemaTables.stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private Queue<JdbcConnection> YaShanCreateConnectionPool(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> ctx) throws SQLException {
        Queue<JdbcConnection> connectionPool = new ConcurrentLinkedQueue();
        connectionPool.add(this.jdbcConnection);
        //int snapshotMaxThreads = Math.max(1, Math.min(this.connectorConfig.getSnapshotMaxThreads(), ctx.capturedTables.size()));
        //--此处代码做了修改
        // 修改主线程*读线程
        int snapshotMaxThreads = Math.max(DEFAULT_THREAD_COUNT, Math.min(MAX_THREAD_COUNT, this.connectorConfig.getSnapshotMaxThreads() * this.connectorConfig.getTableReadThreads()));
        if (snapshotMaxThreads > 1) {
            Optional<String> firstQuery = this.getSnapshotConnectionFirstSelect(ctx, (TableId) ctx.capturedTables.iterator().next());

            for (int i = 1; i < snapshotMaxThreads; ++i) {
                JdbcConnection conn = this.jdbcConnectionFactory.newConnection().setAutoCommit(false);
                conn.connection().setTransactionIsolation(this.jdbcConnection.connection().getTransactionIsolation());
                this.connectionPoolConnectionCreated(ctx, conn);
                connectionPool.add(conn);
                if (firstQuery.isPresent()) {
                    conn.execute(new String[]{(String) firstQuery.get()});
                }
            }
        }

        LOGGER.info("Created connection pool with {} threads", snapshotMaxThreads);
        return connectionPool;
    }

    private void YaShanRollbackTransaction(Connection connection) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public Optional<String> YaShanDetermineSnapshotOverridesSelect(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
                                                                   TableId tableId,
                                                                   Map<DataCollectionId, String> snapshotSelectOverridesByTable) {
        String overriddenSelect = this.getSnapshotSelectOverridesByTable(tableId, snapshotSelectOverridesByTable);
        if (overriddenSelect != null) {
            return Optional.of(this.enhanceOverriddenSelect(snapshotContext, overriddenSelect, tableId));
        }
        return null;
    }

    private Optional<String> YaShanDetermineSnapshotSelect(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
                                                           TableId tableId,
                                                           Map<DataCollectionId, String> snapshotSelectOverridesByTable) {
        String overriddenSelect = this.getSnapshotSelectOverridesByTable(tableId, snapshotSelectOverridesByTable);
        if (overriddenSelect != null) {
            return Optional.of(this.enhanceOverriddenSelect(snapshotContext, overriddenSelect, tableId));
        } else {
            List<String> columns = this.getPreparedColumnNames(snapshotContext.partition, this.databaseSchema.tableFor(tableId));
            return this.getSnapshotSelect(snapshotContext, tableId, columns);
        }
    }

    public Threads.Timer YaShanGetTableScanLogTimer() {
        return Threads.timer(this.clock, LOG_INTERVAL);
    }

    private void YaShanSetSnapshotMarker(OffsetContext offset,
                                         boolean firstTable,
                                         boolean lastTable,
                                         boolean firstRecordInTable,
                                         boolean lastRecordInTable) {
        if (lastRecordInTable && lastTable) {
            offset.markSnapshotRecord(SnapshotRecord.LAST);
        } else if (firstRecordInTable && firstTable) {
            offset.markSnapshotRecord(SnapshotRecord.FIRST);
        } else if (lastRecordInTable) {
            offset.markSnapshotRecord(SnapshotRecord.LAST_IN_DATA_COLLECTION);
        } else if (firstRecordInTable) {
            offset.markSnapshotRecord(SnapshotRecord.FIRST_IN_DATA_COLLECTION);
        } else {
            offset.markSnapshotRecord(SnapshotRecord.TRUE);
        }

    }

    // 新增方法：处理分片快照标记
    public void setSnapshotMarkerForChunk(OffsetContext offset,
                                          boolean firstTable, boolean lastTable,
                                          boolean firstChunkInTable, boolean lastChunkInTable,
                                          boolean firstRecordInChunk, boolean lastRecordInChunk) {
        // 处理整个快照的第一个记录
        if (firstRecordInChunk && firstChunkInTable && firstTable) {
            offset.markSnapshotRecord(SnapshotRecord.FIRST);
        }
        // 处理整个快照的最后一个记录
        else if (lastRecordInChunk && lastChunkInTable && lastTable) {
            offset.markSnapshotRecord(SnapshotRecord.LAST);
        }
        // 处理表的第一个记录（但不是整个快照的第一个）
        else if (firstRecordInChunk && firstChunkInTable) {
            offset.markSnapshotRecord(SnapshotRecord.FIRST_IN_DATA_COLLECTION);
        }
        // 处理表的最后一个记录（但不是整个快照的最后一个）
        else if (lastRecordInChunk && lastChunkInTable) {
            offset.markSnapshotRecord(SnapshotRecord.LAST_IN_DATA_COLLECTION);
        }
        // 处理分片的第一个记录（但不是表的第一个）
        else if (firstRecordInChunk) {
            // 可以添加新的标记类型或使用现有标记
            offset.markSnapshotRecord(SnapshotRecord.TRUE);
        }
        // 处理分片的最后一个记录（但不是表的最后一个）
        else if (lastRecordInChunk) {
            // 可以添加新的标记类型或使用现有标记
            offset.markSnapshotRecord(SnapshotRecord.TRUE);
        }
        // 普通记录
        else {
            offset.markSnapshotRecord(SnapshotRecord.TRUE);
        }
    }

    public void YaShanDoCreateDataEventsForTable(ChangeEventSourceContext sourceContext,
                                                 YashanDBPartition partition,
                                                 YashanDBOffsetContext offset,
                                                 EventDispatcher.SnapshotReceiver<YashanDBPartition> snapshotReceiver,
                                                 Table table,
                                                 boolean firstTable,
                                                 boolean lastTable,
                                                 int tableOrder,
                                                 int tableCount,
                                                 String selectStatement,
                                                 OptionalLong rowCount,
                                                 JdbcConnection jdbcConnection,
                                                 boolean isFirstChunk,
                                                 boolean isLastChunk,
                                                 int ProcessesNumber) throws InterruptedException {
        if (!sourceContext.isRunning()) {
            throw new InterruptedException("Interrupted while snapshotting table " + table.id());
        } else {
            long exportStart = this.clock.currentTimeInMillis();
            LOGGER.info("Processes {} is exporting data from table '{}' ({} of {} tables  sql:{})", new Object[]{ProcessesNumber, table.id(), tableOrder, tableCount, selectStatement});
            Instant sourceTableSnapshotTimestamp = this.getSnapshotSourceTimestamp(jdbcConnection, offset, table.id());

            try {
                try (
                        Statement statement = this.readTableStatement(jdbcConnection, rowCount);
                        ResultSet rs = CancellableResultSet.from(statement.executeQuery(selectStatement));
                ) {
                    ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
                    long rows = 0L;
                    Threads.Timer logTimer = this.YaShanGetTableScanLogTimer();
                    boolean hasNext = rs.next();
                    if (hasNext) {
                        while (hasNext) {
                            if (!sourceContext.isRunning()) {
                                throw new InterruptedException("Interrupted while snapshotting table " + table.id());
                            }
                            ++rows;
                            Object[] row = jdbcConnection.rowToArray(table, rs, columnArray);
                            if (logTimer.expired()) {
                                long stop = this.clock.currentTimeInMillis();
                                if (rowCount.isPresent()) {
                                    LOGGER.info("\t Processes {} : Exported {} of {} records for table '{}' after {}", new Object[]{ProcessesNumber, rows, rowCount.getAsLong(), table.id(), Strings.duration(stop - exportStart)});
                                } else {
                                    LOGGER.info("\t Processes {} : Exported {} records for table '{}' after {}", new Object[]{ProcessesNumber, rows, table.id(), Strings.duration(stop - exportStart)});
                                }
                                this.snapshotProgressListener.rowsScanned(partition, table.id(), rows);
                                logTimer = this.YaShanGetTableScanLogTimer();
                            }
                            hasNext = rs.next();
                            this.setSnapshotMarkerForChunk(offset, firstTable, lastTable, rows == 1L, !hasNext, isFirstChunk, isLastChunk);
                            this.dispatcher.dispatchSnapshotEvent(partition, table.id(), this.getChangeRecordEmitter(partition, offset, table.id(), row, sourceTableSnapshotTimestamp), snapshotReceiver);
                        }
                    } else {
                        this.setSnapshotMarkerForChunk(offset, firstTable, lastTable, false, true, isFirstChunk, isLastChunk);
                    }
                    LOGGER.info("\t Processes {} : Finished exporting {} records for table '{}' ({} of {} tables); total duration '{}'", new Object[]{ProcessesNumber, rows, table.id(), tableOrder, tableCount, Strings.duration(this.clock.currentTimeInMillis() - exportStart)});
                    if (snapshotProgressListener instanceof io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics) {
                        io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics<YashanDBPartition> metrics = (io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics<YashanDBPartition>) snapshotProgressListener;
                        ConcurrentMap<String, Long> rowsScanned = metrics.getRowsScanned();
                        if (null != rowsScanned)
                            rowsScanned.merge(table.id().identifier(), rows, Long::sum);
                    } else {
                        LOGGER.warn("Processes {} : snapshotProgressListener is not an instance of DefaultSnapshotChangeEventSourceMetrics, cannot get rowsScanned map.", ProcessesNumber);
                    }
                }

            } catch (SQLException e) {
                throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
            }
        }
    }

    private Callable<Void> YaShanCreateDataEventsForTableCallable(ChangeEventSourceContext sourceContext,
                                                                  RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
                                                                  EventDispatcher.SnapshotReceiver<YashanDBPartition> snapshotReceiver,
                                                                  Table table,
                                                                  boolean firstTable,
                                                                  boolean lastTable,
                                                                  int tableOrder,
                                                                  int tableCount,
                                                                  String selectStatement,
                                                                  OptionalLong rowCount,
                                                                  Queue<JdbcConnection> connectionPool,
                                                                  Queue<YashanDBOffsetContext> offsets,
                                                                  boolean isFirstChunk,
                                                                  boolean isLastChunk,
                                                                  int ProcessesNumber) {
        return () -> {
            JdbcConnection connection = (JdbcConnection) connectionPool.poll();
            YashanDBOffsetContext offset = (YashanDBOffsetContext) (offsets.poll());

            try {
                this.YaShanDoCreateDataEventsForTable(sourceContext, snapshotContext.partition, offset, snapshotReceiver, table, firstTable, lastTable, tableOrder, tableCount, selectStatement, rowCount, connection, isFirstChunk, isLastChunk, ProcessesNumber);
            } finally {
                offsets.add(offset);
                connectionPool.add(connection);
            }

            return null;
        };
    }

    private void YaShanCreateDataEvents(ChangeEventSourceContext sourceContext,
                                        RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
                                        Queue<JdbcConnection> connectionPool,
                                        Map<DataCollectionId, String> snapshotSelectOverridesByTable) throws Exception {
        this.tryStartingSnapshot(snapshotContext);
        EventDispatcher.SnapshotReceiver<YashanDBPartition> snapshotReceiver = this.dispatcher.getSnapshotChangeEventReceiver();
        int snapshotMaxThreads = connectionPool.size();
        LOGGER.info("Creating snapshot worker pool with {} worker thread(s)", snapshotMaxThreads);
        ExecutorService executorService = Executors.newFixedThreadPool(snapshotMaxThreads);
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

        Map<TableId, ArrayList<String>> queryTablesSql = new HashMap<>();
        Map<TableId, OptionalLong> rowCountTables = new LinkedHashMap<>();

        for (TableId tableId : snapshotContext.capturedTables) {
            final Map<String, Integer> datafileInfo = queryDatafileInfo();
            queryTablesSql.put(tableId, new ArrayList<>());
            final SnapshotTableSplitInfo tableInfo = ((YashanDBSnapshotContext) snapshotContext).tableSplitMap.get(tableId);
            //优先处理用户指定的数据迁移sql，用户一旦进行了配置，那么就不对表进行分片迁移
            Optional<String> selectStatement = this.YaShanDetermineSnapshotOverridesSelect(snapshotContext, tableId, snapshotSelectOverridesByTable);
            if (null != selectStatement && selectStatement.isPresent()) {
                LOGGER.info("For table '{}' using select statement: '{}'", tableId, selectStatement.get());
                queryTablesSql.get(tableId).add(selectStatement.get());
            } else {
                //先对分区表进行处理
                if (tableInfo.getPartitionInfo().getIsPartition()) {
                    partitionSplitTable(snapshotContext.offset.getScn().toString(),
                            ((YashanDBSnapshotContext) snapshotContext).tableSplitMap.get(tableId),
                            snapshotMaxThreads,
                            datafileInfo,
                            queryTablesSql);
                } else {
                    //对非分区表进行处理
                    notPartitionSplitTable(snapshotContext.offset.getScn().toString(),
                            ((YashanDBSnapshotContext) snapshotContext).tableSplitMap.get(tableId),
                            snapshotMaxThreads,
                            datafileInfo,
                            queryTablesSql);
                }
            }
            OptionalLong rowCount = this.rowCountForTable(tableId);
            rowCountTables.put(tableId, rowCount);
        }

        // TODO 更换数据结构动态排序 (没必要）
        if (this.connectorConfig.snapshotOrderByRowCount() != RelationalDatabaseConnectorConfig.SnapshotTablesRowCountOrder.DISABLED) {
            LOGGER.info("Sort tables by row count '{}'", this.connectorConfig.snapshotOrderByRowCount());
            int orderFactor = this.connectorConfig.snapshotOrderByRowCount() == RelationalDatabaseConnectorConfig.SnapshotTablesRowCountOrder.ASCENDING ? 1 : -1;
            rowCountTables = (Map) rowCountTables.entrySet().stream().sorted(Map.Entry.comparingByValue((a, b) -> orderFactor * Long.compare(a.orElse(0L), b.orElse(0L)))).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        Queue<YashanDBOffsetContext> offsets = new ConcurrentLinkedQueue<>();
        offsets.add(snapshotContext.offset);

        for (int i = 1; i < snapshotMaxThreads; ++i) {
            offsets.add(this.copyOffset(snapshotContext));
        }

        try {
            int tableCount = rowCountTables.size();
            int tableOrder = 1;
            int asyProcessCount = 0;   //异步处理过程个数

            long exportStart = this.clock.currentTimeInMillis();

            for (TableId tableId : rowCountTables.keySet()) {
                //新处理方式
                boolean firstTable = tableOrder == 1 && snapshotMaxThreads == 1;
                boolean lastTable = tableOrder == tableCount && snapshotMaxThreads == 1;
                OptionalLong rowCount = (OptionalLong) rowCountTables.get(tableId);
                ArrayList<String> sqlList = queryTablesSql.get(tableId);

                // 获取该表的Chunk总数
                int chunkCount = sqlList.size();
                int currentChunkIndex = 0;

                for (String selectStatement : sqlList) {
                    currentChunkIndex++;
                    // 判断是否是第一个Chunk
                    boolean isFirstChunk = (currentChunkIndex == 1);
                    // 判断是否是最后一个Chunk
                    boolean isLastChunk = (currentChunkIndex == chunkCount);
                    Callable<Void> callable = this.YaShanCreateDataEventsForTableCallable(sourceContext, snapshotContext, snapshotReceiver, snapshotContext.tables.forTable(tableId), firstTable, lastTable, tableOrder, tableCount, selectStatement, rowCount, connectionPool, offsets, isFirstChunk, isLastChunk, asyProcessCount + 1);
                    completionService.submit(callable);
                    asyProcessCount++;
                }
                tableOrder++;
            }

            for (int i = 0; i < asyProcessCount; ++i) {
                completionService.take().get();
            }
            LOGGER.info("finish all table data,table count: {},thread count:{},number of Processes:{},time:{}", tableCount, snapshotMaxThreads, asyProcessCount, Strings.duration(this.clock.currentTimeInMillis() - exportStart));
            //所有线程处理完成了，数据也同步完成了，设置统计主题
            if (snapshotProgressListener instanceof io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics) {
                io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics<YashanDBPartition> metrics = (io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics<YashanDBPartition>) snapshotProgressListener;
                ConcurrentMap<String, Long> rowsScanned = metrics.getRowsScanned();
                if (null != rowsScanned) {
                    for (TableId tableId : rowCountTables.keySet()) {
                        Long rowCount = rowsScanned.get(tableId.identifier());
                        if (null != rowCount)
                            this.snapshotProgressListener.dataCollectionSnapshotCompleted(snapshotContext.partition, tableId, rowCount);
                    }
                }
            } else {
                LOGGER.warn("snapshotProgressListener is not an instance of DefaultSnapshotChangeEventSourceMetrics,no set snapshot completed");
            }
        } finally {
            executorService.shutdownNow();
        }

        this.releaseDataSnapshotLocks(snapshotContext);

        for (YashanDBOffsetContext offset : offsets) {
            offset.preSnapshotCompletion();
        }

        snapshotReceiver.completeSnapshot();

        for (YashanDBOffsetContext offset : offsets) {
            offset.postSnapshotCompletion();
        }

    }

    public void releaseDataSnapshotLocks(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext) throws Exception {
    }

    public Statement readTableStatement(JdbcConnection jdbcConnection, OptionalLong tableSize) throws SQLException {
        return jdbcConnection.readTableStatement(connectorConfig, tableSize);
    }

    public ChangeRecordEmitter<YashanDBPartition> getChangeRecordEmitter(YashanDBPartition partition, YashanDBOffsetContext offset, TableId tableId,
                                                                         Object[] row, Instant timestamp) {
        offset.event(tableId, timestamp);
        return new SnapshotChangeRecordEmitter<>(partition, offset, row, getClock(), connectorConfig);
    }

    public void tryStartingSnapshot(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext) {
        if (!snapshotContext.offset.isSnapshotRunning()) {
            snapshotContext.offset.preSnapshotStart();
        }
    }

    public SnapshotResult<YashanDBOffsetContext> doExecute(ChangeEventSourceContext context,
                                                           YashanDBOffsetContext previousOffset,
                                                           SnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
                                                           SnapshottingTask snapshottingTask) throws Exception {
        YashanDBSnapshotContext ctx = (YashanDBSnapshotContext) snapshotContext;
        Connection connection = null;
        Throwable exceptionWhileSnapshot = null;

        SnapshotResult var10;
        try {
            Set<Pattern> dataCollectionsToBeSnapshotted = this.getDataCollectionPattern(snapshottingTask.getDataCollections());
            Map<DataCollectionId, String> snapshotSelectOverridesByTable = (Map) snapshottingTask.getFilterQueries().entrySet().stream().collect(Collectors.toMap((ex) -> TableId.parse((String) ex.getKey()), Map.Entry::getValue));
            this.preSnapshot();
            LOGGER.info("Snapshot step 1 - Preparing");
            if (previousOffset != null && previousOffset.isSnapshotRunning()) {
                LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
            }

            connection = this.createSnapshotConnection();
            this.connectionCreated(ctx);
            LOGGER.info("Snapshot step 2 - Determining captured tables");
            this.YaShanDetermineCapturedTables(ctx, dataCollectionsToBeSnapshotted, snapshottingTask);
            this.snapshotProgressListener.monitoredDataCollectionsDetermined(snapshotContext.partition, ctx.capturedTables);
            this.connectionPool = this.YaShanCreateConnectionPool(ctx);
            LOGGER.info("Snapshot step 3 - Locking captured tables {}", ctx.capturedTables);
            if (snapshottingTask.snapshotSchema()) {
                this.lockTablesForSchemaSnapshot(context, ctx);
            }

            LOGGER.info("Snapshot step 4 - Determining snapshot offset");
            this.determineSnapshotOffset(ctx, previousOffset);
            LOGGER.info("Snapshot step 5 - Reading structure of captured tables");
            this.readTableStructure(context, ctx, previousOffset, snapshottingTask);
            if (snapshottingTask.snapshotSchema()) {
                LOGGER.info("Snapshot step 6 - Persisting schema history");
                this.createSchemaChangeEventsForTables(context, ctx, snapshottingTask);
                this.releaseSchemaSnapshotLocks(ctx);
            } else {
                LOGGER.info("Snapshot step 6 - Skipping persisting of schema history");
            }

            if (snapshottingTask.snapshotData()) {
                LOGGER.info("Snapshot step 7 - Snapshotting data");
//                this.YaShanCreateDataEvents(context, ctx, this.connectionPool, snapshotSelectOverridesByTable);
                SnapshotDataSyncTask.SyncTaskContext syncTaskContext = new SnapshotDataSyncTask.SyncTaskContext(
                        (YashanDBSnapshotContext) snapshotContext, context,
                        connectionPool, snapshotSelectOverridesByTable,
                        connectorConfig, this, clock
                );
                SnapshotDataSyncTask snapshotDataSyncTask = new SnapshotDataSyncTask(syncTaskContext);
                snapshotDataSyncTask.run();
            } else {
                LOGGER.info("Snapshot step 7 - Skipping snapshotting of data");
                this.releaseDataSnapshotLocks(ctx);
                ctx.offset.preSnapshotCompletion();
                ctx.offset.postSnapshotCompletion();
            }

            this.postSnapshot();
            this.dispatcher.alwaysDispatchHeartbeatEvent(ctx.partition, ctx.offset);
            var10 = SnapshotResult.completed(ctx.offset);
        } catch (AssertionError | Exception e) {
            LOGGER.error("Error during snapshot", e);
            exceptionWhileSnapshot = e;
            throw e;
        } finally {
            try {
                if (this.connectionPool != null) {
                    for (JdbcConnection conn : this.connectionPool) {
                        if (!this.jdbcConnection.equals(conn)) {
                            conn.close();
                        }
                    }
                }

                this.YaShanRollbackTransaction(connection);
            } catch (Exception var20) {
                LOGGER.error("Error in finally block", var20);
                if (exceptionWhileSnapshot != null) {
                    var20.addSuppressed(exceptionWhileSnapshot);
                }

                throw var20;
            }
        }

        return var10;
    }

    public RowIds getRowIdsByView(String sql) throws SQLException {
        JdbcConnection connection = this.connectionPool.poll();
        try {
            JdbcConnection validConnection = connection == null ? this.jdbcConnection : connection;
            try (Statement statement = this.readTableStatement(validConnection, OptionalLong.empty());
                 ResultSet rs = statement.executeQuery(sql)) {
                if (rs.next()) {
                    String min = rs.getString("MIN_ROWID");
                    String max = rs.getString("MAX_ROWID");
                    return new RowIds(min, max);
                }
            }
            return null;
        } finally {
            if (connection != null) {
                this.connectionPool.add(connection);
            }
        }
    }

    public static class RowIds {

        private final String minRowId;

        private final String maxRowId;

        public RowIds(String minRowId, String maxRowId) {
            this.minRowId = minRowId;
            this.maxRowId = maxRowId;
        }
    }

}
