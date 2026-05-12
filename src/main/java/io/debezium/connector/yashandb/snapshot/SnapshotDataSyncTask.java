package io.debezium.connector.yashandb.snapshot;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.yashandb.YashanDBConnectorConfig;
import io.debezium.connector.yashandb.YashanDBOffsetContext;
import io.debezium.connector.yashandb.YashanDBPartition;
import io.debezium.connector.yashandb.YashanDBSnapshotChangeEventSource;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

/**
 * Snapshot data sync task.
 */
public class SnapshotDataSyncTask {

    private static final Logger log = LoggerFactory.getLogger(SnapshotDataSyncTask.class);

    private final SyncTaskContext syncTaskContext;
    private final ThreadPoolExecutor executor;

    public SnapshotDataSyncTask(SyncTaskContext syncTaskContext) {
        this.syncTaskContext = syncTaskContext;
        this.executor = new ThreadPoolExecutor(syncTaskContext.getConnectorConfig().getSnapshotMaxThreads(), syncTaskContext.getConnectorConfig().getSnapshotMaxThreads(),
                5, TimeUnit.SECONDS, new ArrayBlockingQueue<>(syncTaskContext.getSnapshotContext().capturedTables.size()));
    }

    public void run() throws Exception {
        try {
            RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext = syncTaskContext
                    .getSnapshotContext();
            syncTaskContext.getEventSource().tryStartingSnapshot(snapshotContext);
            int tableCount = syncTaskContext.getSnapshotContext().capturedTables.size();
            int snapshotMaxThreads = syncTaskContext.getConnectorConfig().getSnapshotMaxThreads();
            int tableReadThreads = syncTaskContext.getConnectorConfig().getTableReadThreads();
            final Clock clock = syncTaskContext.getClock();
            long exportStart = clock.currentTimeInMillis();
            YashanDBSnapshotChangeEventSource eventSource = syncTaskContext.getEventSource();
            EventDispatcher.SnapshotReceiver<YashanDBPartition> snapshotReceiver = eventSource.dispatcher.getSnapshotChangeEventReceiver();

            Queue<YashanDBOffsetContext> offsets = new ConcurrentLinkedQueue<>();
            offsets.add(snapshotContext.offset);
            for (int i = 1; i < snapshotMaxThreads * tableReadThreads; ++i) {
                offsets.add(eventSource.copyOffset(snapshotContext));
            }

            log.info("Creating snapshot worker pool with {} worker thread(s)", snapshotMaxThreads);
            log.info("Plan to sync {} tables, enabled logic shard: {}", tableCount, syncTaskContext.getConnectorConfig().getLogicShardEnabled());
            // 1.将每张表组装成作业线程并发执行
            for (TableId tableId : snapshotContext.capturedTables) {
                executor.execute(new TableDataSyncTask(tableId, syncTaskContext, offsets, snapshotReceiver));
            }
            // 2.并发管理
            syncTaskContext.getParentLatch().await();
            // 3.置后处理
            afterProcess(offsets, snapshotReceiver);
            if (!syncTaskContext.getErrorTables().isEmpty()) {
                log.error("Snapshot sync failed tables: {}", syncTaskContext.getErrorTables());
                throw new RuntimeException("Snapshot stage synchronization failed !!!");
            }
            log.info("All table data sync finished,plan table count: {},finished table count: {}, time:{}",
                    tableCount, syncTaskContext.getFinishedCount().get(), Strings.duration(clock.currentTimeInMillis() - exportStart));
        }
        catch (Exception e) {
            log.error("Snapshot sync error: ", e);
            throw e;
        }
    }

    private void afterProcess(Queue<YashanDBOffsetContext> offsets, EventDispatcher.SnapshotReceiver<YashanDBPartition> snapshotReceiver) throws Exception {
        // 所有线程处理完成了，数据也同步完成了，设置统计主题
        YashanDBSnapshotChangeEventSource eventSource = syncTaskContext.getEventSource();
        RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext = syncTaskContext.getSnapshotContext();
        if (eventSource.snapshotProgressListener instanceof io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics) {
            io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics<YashanDBPartition> metrics = (io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics<YashanDBPartition>) eventSource.snapshotProgressListener;
            java.util.concurrent.ConcurrentMap<String, Long> rowsScanned = metrics.getRowsScanned();
            if (null != rowsScanned) {
                for (TableId tableId : snapshotContext.capturedTables) {
                    Long rowCount = rowsScanned.get(tableId.identifier());
                    if (null != rowCount)
                        eventSource.snapshotProgressListener.dataCollectionSnapshotCompleted(snapshotContext.partition, tableId, rowCount);
                }
            }
        }
        else {
            log.warn("snapshotProgressListener is not an instance of DefaultSnapshotChangeEventSourceMetrics,no set snapshot completed");
        }

        eventSource.releaseDataSnapshotLocks(snapshotContext);

        for (YashanDBOffsetContext offset : offsets) {
            offset.preSnapshotCompletion();
        }

        snapshotReceiver.completeSnapshot();

        for (YashanDBOffsetContext offset : offsets) {
            offset.postSnapshotCompletion();
        }
    }

    public static class SyncTaskContext {

        private final RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext;
        private final ChangeEventSource.ChangeEventSourceContext sourceContext;
        private final Queue<JdbcConnection> connectionPool;
        private final Map<DataCollectionId, String> snapshotSelectOverridesByTable;
        private final YashanDBConnectorConfig connectorConfig;
        private final YashanDBSnapshotChangeEventSource eventSource;
        private final CountDownLatch parentLatch;
        private final List<String> errorTables = new CopyOnWriteArrayList<>();
        private final AtomicInteger finishedCount = new AtomicInteger(0);
        private final Clock clock;

        public SyncTaskContext(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
                               ChangeEventSource.ChangeEventSourceContext sourceContext,
                               Queue<JdbcConnection> connectionPool, Map<DataCollectionId, String> snapshotSelectOverridesByTable,
                               YashanDBConnectorConfig connectorConfig,
                               YashanDBSnapshotChangeEventSource eventSource, Clock clock) {
            this.snapshotContext = snapshotContext;
            this.sourceContext = sourceContext;
            this.connectionPool = connectionPool;
            this.snapshotSelectOverridesByTable = snapshotSelectOverridesByTable;
            this.connectorConfig = connectorConfig;
            this.eventSource = eventSource;
            this.parentLatch = new CountDownLatch(snapshotContext.capturedTables.size());
            this.clock = clock;
        }

        public RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> getSnapshotContext() {
            return snapshotContext;
        }

        public ChangeEventSource.ChangeEventSourceContext getSourceContext() {
            return sourceContext;
        }

        public Queue<JdbcConnection> getConnectionPool() {
            return connectionPool;
        }

        public Map<DataCollectionId, String> getSnapshotSelectOverridesByTable() {
            return snapshotSelectOverridesByTable;
        }

        public YashanDBConnectorConfig getConnectorConfig() {
            return connectorConfig;
        }

        public YashanDBSnapshotChangeEventSource getEventSource() {
            return eventSource;
        }

        public CountDownLatch getParentLatch() {
            return parentLatch;
        }

        public List<String> getErrorTables() {
            return errorTables;
        }

        public Clock getClock() {
            return clock;
        }

        public AtomicInteger getFinishedCount() {
            return finishedCount;
        }
    }
}
