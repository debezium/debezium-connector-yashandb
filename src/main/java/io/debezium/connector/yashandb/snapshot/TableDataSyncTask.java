package io.debezium.connector.yashandb.snapshot;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.yashandb.SnapshotTableSplitInfo;
import io.debezium.connector.yashandb.YaShanDBPartitionInfo;
import io.debezium.connector.yashandb.YashanDBConnectorConfig;
import io.debezium.connector.yashandb.YashanDBOffsetContext;
import io.debezium.connector.yashandb.YashanDBPartition;
import io.debezium.connector.yashandb.YashanDBSnapshotChangeEventSource;
import io.debezium.jdbc.CancellableResultSet;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

/**
 * One table sync task.
 */
public class TableDataSyncTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TableDataSyncTask.class);

    private final TableId tableId;
    private final SnapshotDataSyncTask.SyncTaskContext syncTaskContext;
    private final ThreadPoolExecutor executor;
    private final Queue<YashanDBOffsetContext> offsets;
    private final EventDispatcher.SnapshotReceiver<YashanDBPartition> snapshotReceiver;

    private final AtomicLong blockSendTime = new AtomicLong(0L);

    public TableDataSyncTask(TableId tableId, SnapshotDataSyncTask.SyncTaskContext syncTaskContext,
                             Queue<YashanDBOffsetContext> offsets, EventDispatcher.SnapshotReceiver<YashanDBPartition> snapshotReceiver) {
        this.tableId = tableId;
        this.syncTaskContext = syncTaskContext;
        this.executor = new ThreadPoolExecutor(syncTaskContext.getConnectorConfig().getTableReadThreads(),
                syncTaskContext.getConnectorConfig().getTableReadThreads(),
                5,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory(tableId.schema() + "@" + tableId.table() + "_sub_thread_pool_")); // empty
        this.offsets = offsets;
        this.snapshotReceiver = snapshotReceiver;
    }

    @Override
    public void run() {
        try {
            // 1. 准备SQL
            List<String> sqlList = prepareSQL();
            // 2. 并发抽取
            int subShardCnt = sqlList.size();
            final CountDownLatch subLatch = new CountDownLatch(subShardCnt);
            for (String sql : sqlList) {
                executor.execute(new SyncWorker(syncTaskContext, subLatch, sql, tableId, offsets, snapshotReceiver, blockSendTime));
            }
            subLatch.await();
            log.info("Table {} sync finished and has been blocked for {} (s)", tableId, blockSendTime.get() / 1000);
        }
        catch (Exception e) {
            log.error("Table {} sync error", tableId.toString(), e);
            syncTaskContext.getErrorTables().add(tableId.toString());
        }
        finally {
            syncTaskContext.getFinishedCount().incrementAndGet();
            syncTaskContext.getParentLatch().countDown();
            executor.shutdown();
        }
    }

    private List<String> prepareSQL() throws Exception {
        YashanDBConnectorConfig connectorConfig = syncTaskContext.getConnectorConfig();
        YashanDBSnapshotChangeEventSource eventSource = syncTaskContext.getEventSource();
        RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext = syncTaskContext.getSnapshotContext();
        // 1.用户配置SQL优先级最高
        // 为复用方法，数据结构保留
        Map<TableId, ArrayList<String>> querySQL = new HashMap<>();
        querySQL.put(tableId, new ArrayList<>());
        Optional<String> selectStatement = eventSource.YaShanDetermineSnapshotOverridesSelect(snapshotContext, tableId,
                syncTaskContext.getSnapshotSelectOverridesByTable());
        if (null != selectStatement && selectStatement.isPresent()) {
            log.info("For table '{}' using select statement: '{}'", tableId, selectStatement.get());
            querySQL.get(tableId).add(selectStatement.get());
            return querySQL.get(tableId);
        }

        // 2.启用逻辑分片
        if (connectorConfig.getLogicShardEnabled()) {
            return logicShardSQL(querySQL);
        }
        else {
            // 3.未启用逻辑分片
            return noLogicShardSQL();
        }
    }

    private List<String> logicShardSQL(Map<TableId, ArrayList<String>> querySQL) throws Exception {
        RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext = syncTaskContext.getSnapshotContext();
        YashanDBSnapshotChangeEventSource eventSource = syncTaskContext.getEventSource();
        int tableReadThreads = syncTaskContext.getConnectorConfig().getTableReadThreads();
        SnapshotTableSplitInfo tableInfo = ((YashanDBSnapshotChangeEventSource.YashanDBSnapshotContext) snapshotContext).tableSplitMap.get(tableId);
        Map<String, Integer> datafileInfo = eventSource.queryDatafileInfo();
        // 先对分区表进行处理
        if (tableInfo.getPartitionInfo().getIsPartition()) {
            eventSource.partitionSplitTable(snapshotContext.offset.getScn().toString(),
                    ((YashanDBSnapshotChangeEventSource.YashanDBSnapshotContext) snapshotContext).tableSplitMap.get(tableId),
                    tableReadThreads,
                    datafileInfo,
                    querySQL);
        }
        else {
            // 对非分区表进行处理
            eventSource.notPartitionSplitTable(snapshotContext.offset.getScn().toString(),
                    ((YashanDBSnapshotChangeEventSource.YashanDBSnapshotContext) snapshotContext).tableSplitMap.get(tableId),
                    tableReadThreads,
                    datafileInfo,
                    querySQL);
        }
        return querySQL.get(tableId);
    }

    private List<String> noLogicShardSQL() {
        YashanDBSnapshotChangeEventSource eventSource = syncTaskContext.getEventSource();
        RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext = syncTaskContext.getSnapshotContext();
        SnapshotTableSplitInfo tableInfo = ((YashanDBSnapshotChangeEventSource.YashanDBSnapshotContext) snapshotContext).tableSplitMap.get(tableId);
        String columnNames = tableInfo.getColumnNames().stream().map(eventSource::quoteIdentifier).collect(Collectors.joining(","));
        List<String> querySQL = new ArrayList<>();
        // 分区表拆分区
        if (tableInfo.getPartitionInfo().getIsPartition()) {
            List<YaShanDBPartitionInfo.SubPartitionInfo> subPartitionInfo = tableInfo.getPartitionInfo().getSubPartitionInfo();
            for (YaShanDBPartitionInfo.SubPartitionInfo partitionInfo : subPartitionInfo) {
                querySQL.add(String.format(SnapshotSQLConstants.FLASH_BACK_SELECT_PARTITION,
                        columnNames,
                        eventSource.getObjectName(tableId),
                        partitionInfo.partitionName(),
                        snapshotContext.offset.getScn().toString()));
            }
        }
        else {
            // 单线程读取
            querySQL.add(String.format(SnapshotSQLConstants.FLASH_BACK_SELECT_NO_PARTITION,
                    columnNames,
                    eventSource.getObjectName(tableId),
                    snapshotContext.offset.getScn().toString()));
        }
        return querySQL;
    }

    private static class SyncWorker implements Runnable {

        private final SnapshotDataSyncTask.SyncTaskContext syncTaskContext;
        private final CountDownLatch subLatch;
        private final String sql;
        private final TableId tableId;
        private final Queue<YashanDBOffsetContext> offsets;
        private final EventDispatcher.SnapshotReceiver<YashanDBPartition> snapshotReceiver;

        private final AtomicLong blockSendTime;

        private SyncWorker(SnapshotDataSyncTask.SyncTaskContext syncTaskContext, CountDownLatch subLatch,
                           String sql, TableId tableId, Queue<YashanDBOffsetContext> offsets,
                           EventDispatcher.SnapshotReceiver<YashanDBPartition> snapshotReceiver,
                           AtomicLong blockSendTime) {
            this.syncTaskContext = syncTaskContext;
            this.subLatch = subLatch;
            this.sql = sql;
            this.tableId = tableId;
            this.offsets = offsets;
            this.snapshotReceiver = snapshotReceiver;
            this.blockSendTime = blockSendTime;
        }

        @Override
        public void run() {
            Queue<JdbcConnection> connectionPool = syncTaskContext.getConnectionPool();
            JdbcConnection jdbcConnection = connectionPool.poll();
            YashanDBOffsetContext offset = offsets.poll();
            YashanDBSnapshotChangeEventSource eventSource = syncTaskContext.getEventSource();
            RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext = syncTaskContext
                    .getSnapshotContext();
            ChangeEventSource.ChangeEventSourceContext sourceContext = syncTaskContext.getSourceContext();
            final Table table = snapshotContext.tables.forTable(tableId);
            final Clock clock = syncTaskContext.getClock();
            final YashanDBPartition partition = snapshotContext.partition;
            SnapshotProgressListener<YashanDBPartition> snapshotProgressListener = eventSource.snapshotProgressListener;
            try {
                if (!sourceContext.isRunning()) {
                    throw new InterruptedException("Interrupted while snapshotting table " + table.id());
                }
                else {
                    long exportStart = clock.currentTimeInMillis();
                    log.info("Processes {} is exporting data from table '{}' (sql:{})", Thread.currentThread().getName(), table.id(), sql);
                    assert offset != null;
                    Instant sourceTableSnapshotTimestamp = eventSource.getSnapshotSourceTimestamp(jdbcConnection, offset, table.id());

                    try {
                        assert jdbcConnection != null;
                        // table size not use
                        try (Statement statement = eventSource.readTableStatement(jdbcConnection, OptionalLong.of(-1L));
                                ResultSet rs = CancellableResultSet.from(statement.executeQuery(sql))) {
                            ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
                            long rows = 0L;
                            Threads.Timer logTimer = eventSource.YaShanGetTableScanLogTimer();
                            boolean hasNext = rs.next();
                            if (hasNext) {
                                while (hasNext) {
                                    if (!sourceContext.isRunning()) {
                                        throw new InterruptedException("Interrupted while snapshotting table " + table.id());
                                    }
                                    ++rows;
                                    Object[] row = jdbcConnection.rowToArray(table, rs, columnArray);
                                    if (logTimer.expired()) {
                                        long stop = clock.currentTimeInMillis();
                                        log.info("\t Processes {} : Exported {} records for table '{}' after {}", Thread.currentThread().getName(), rows, table.id(),
                                                Strings.duration(stop - exportStart));
                                        log.info("\t Processes {} : Sender has been blocked for {} (s).", Thread.currentThread().getName(), blockSendTime.get() / 1000);
                                        eventSource.snapshotProgressListener.rowsScanned(partition, table.id(), rows);
                                        logTimer = eventSource.YaShanGetTableScanLogTimer();
                                    }
                                    hasNext = rs.next();
                                    markState(offset, subLatch.getCount() == 1, !hasNext);
                                    long start = System.currentTimeMillis();
                                    eventSource.dispatcher.dispatchSnapshotEvent(partition, table.id(),
                                            eventSource.getChangeRecordEmitter(partition, offset, table.id(), row, sourceTableSnapshotTimestamp), snapshotReceiver);
                                    blockSendTime.addAndGet(System.currentTimeMillis() - start);
                                }
                            }
                            else {
                                markState(offset, subLatch.getCount() == 1, true);
                            }
                            log.info("\t Processes {} : Finished exporting {} records for table '{}' ; total duration '{}'", Thread.currentThread().getName(), rows,
                                    table.id(), Strings.duration(clock.currentTimeInMillis() - exportStart));
                            if (snapshotProgressListener instanceof io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics) {
                                io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics<YashanDBPartition> metrics = (io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics<YashanDBPartition>) snapshotProgressListener;
                                java.util.concurrent.ConcurrentMap<String, Long> rowsScanned = metrics.getRowsScanned();
                                if (null != rowsScanned)
                                    rowsScanned.merge(table.id().identifier(), rows, Long::sum);
                            }
                            else {
                                log.warn(
                                        "Processes {} : snapshotProgressListener is not an instance of DefaultSnapshotChangeEventSourceMetrics, cannot get rowsScanned map.",
                                        Thread.currentThread().getName());
                            }
                        }

                    }
                    catch (SQLException e) {
                        throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
                    }
                }
                log.info("Table {} slice {} sync finished", tableId, sql);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            finally {
                offsets.add(offset);
                subLatch.countDown();
                connectionPool.add(jdbcConnection);
            }
        }

        private void markState(YashanDBOffsetContext offset, boolean isLastChunk, boolean isLastRow) {
            // 每张表的最后一条记录都标记为 LAST_IN_DATA_COLLECTION
            if (isLastChunk && isLastRow) {
                offset.markSnapshotRecord(SnapshotRecord.LAST_IN_DATA_COLLECTION);
            }
        }
    }
}
