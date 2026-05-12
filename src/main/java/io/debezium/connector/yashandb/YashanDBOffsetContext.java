/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sics.ystream.result.LogPosition;
import com.sics.ystream.result.Position;
import com.sics.ystream.result.SystemChangeNumber;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

public class YashanDBOffsetContext extends CommonOffsetContext<SourceInfo> {

    private static final Logger log = LoggerFactory.getLogger(YashanDBOffsetContext.class);

    public static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";
    public static final String SNAPSHOT_PENDING_TRANSACTIONS_KEY = "snapshot_pending_tx";
    public static final String SNAPSHOT_SCN_KEY = "snapshot_scn";
    public static final String YSTREAM_START_SCN_KEY = "ystream_start_scn";
    public static final String YSTREAM_SERVER_CREATE = "ystream_server_create";

    private final Schema sourceInfoSchema;

    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;

    /**
     * SCN that was used for the initial consistent snapshot.
     * <p>
     * We keep track of this field because it's a cutoff for emitting DDL statements,
     * in case we start mining _before_ the snapshot SCN to cover transactions that were
     * ongoing at the time the snapshot was taken.
     */
    private final Scn snapshotScn;

    private final Scn ystreamStartScn;
    private final Position recoverPosition;
    private boolean isCreateServer = false;

    /**
     * Map of (txid, start SCN) for all transactions in progress at the time the
     * snapshot was taken.
     */
    private Map<String, Scn> snapshotPendingTransactions;

    /**
     * Whether a snapshot has been completed or not.
     */
    private boolean snapshotCompleted;

    public YashanDBOffsetContext(YashanDBConnectorConfig connectorConfig, Scn scn, CommitScn commitScn,
                                 Scn snapshotScn, Scn ystreamStartScn, Position recoverPosition, Map<String, Scn> snapshotPendingTransactions,
                                 boolean snapshot, boolean snapshotCompleted, TransactionContext transactionContext,
                                 IncrementalSnapshotContext<TableId> incrementalSnapshotContext, boolean isCreateServer) {
        this(connectorConfig, scn, snapshotScn, ystreamStartScn, recoverPosition, snapshotPendingTransactions, snapshot, snapshotCompleted, transactionContext,
                incrementalSnapshotContext, isCreateServer);
        sourceInfo.setCommitScn(commitScn);
    }

    public YashanDBOffsetContext(YashanDBConnectorConfig connectorConfig, Scn scn,
                                 Scn snapshotScn, Scn ystreamStartScn, Position recoverPosition, Map<String, Scn> snapshotPendingTransactions,
                                 boolean snapshot, boolean snapshotCompleted, TransactionContext transactionContext,
                                 IncrementalSnapshotContext<TableId> incrementalSnapshotContext, boolean isCreateServer) {
        super(new SourceInfo(connectorConfig));
        this.recoverPosition = recoverPosition;
        this.ystreamStartScn = ystreamStartScn;
        sourceInfo.setScn(scn);
        // It is safe to set this value to the supplied SCN, specifically for snapshots.
        // During streaming this value will be updated by the current event handler.
        sourceInfo.setEventScn(scn);
        sourceInfo.setLcrPosition(recoverPosition);
        sourceInfo.setCommitScn(CommitScn.valueOf((String) null));
        sourceInfoSchema = sourceInfo.schema();
        this.isCreateServer = isCreateServer;

        // Snapshot SCN is a new field and may be null in cases where the offsets are being read from
        // and older version of Debezium. In this case, we need to explicitly enforce Scn#NULL usage
        // when the value is null.
        this.snapshotScn = snapshotScn == null ? Scn.NULL : snapshotScn;
        this.snapshotPendingTransactions = snapshotPendingTransactions;

        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
    }

    public static class Builder {

        private YashanDBConnectorConfig connectorConfig;
        private Scn scn;
        private String lcrPosition;
        private boolean snapshot;
        private boolean snapshotCompleted;
        private TransactionContext transactionContext;
        private IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
        private Map<String, Scn> snapshotPendingTransactions;
        private Scn snapshotScn;
        private Scn ystreamStartScn;
        private Position recoverPosition;

        public Builder logicalName(YashanDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
            return this;
        }

        public Builder ystreamStartScn(Scn scn) {
            this.ystreamStartScn = scn;
            return this;
        }

        public Builder recoverPosition(Position recoverPosition) {
            this.recoverPosition = recoverPosition;
            return this;
        }

        public Builder scn(Scn scn) {
            this.scn = scn;
            return this;
        }

        public Builder lcrPosition(String lcrPosition) {
            this.lcrPosition = lcrPosition;
            return this;
        }

        public Builder snapshot(boolean snapshot) {
            this.snapshot = snapshot;
            return this;
        }

        public Builder snapshotCompleted(boolean snapshotCompleted) {
            this.snapshotCompleted = snapshotCompleted;
            return this;
        }

        public Builder transactionContext(TransactionContext transactionContext) {
            this.transactionContext = transactionContext;
            return this;
        }

        public Builder incrementalSnapshotContext(IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
            this.incrementalSnapshotContext = incrementalSnapshotContext;
            return this;
        }

        public Builder snapshotPendingTransactions(Map<String, Scn> snapshotPendingTransactions) {
            this.snapshotPendingTransactions = snapshotPendingTransactions;
            return this;
        }

        public Builder snapshotScn(Scn scn) {
            this.snapshotScn = scn;
            return this;
        }

        public YashanDBOffsetContext build() {
            return new YashanDBOffsetContext(connectorConfig, scn, snapshotScn, ystreamStartScn, recoverPosition, snapshotPendingTransactions, snapshot,
                    snapshotCompleted, transactionContext,
                    incrementalSnapshotContext, false);
        }
    }

    public static Builder create() {
        return new Builder();
    }

    @Override
    public Map<String, ?> getOffset() {
        if (sourceInfo.isSnapshot()) {
            Map<String, Object> offset = new HashMap<>();

            final Scn scn = sourceInfo.getScn();
            offset.put(SourceInfo.SCN_KEY, scn != null ? scn.toString() : null);
            offset.put(SourceInfo.SNAPSHOT_KEY, true);
            offset.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);

            if (snapshotPendingTransactions != null && !snapshotPendingTransactions.isEmpty()) {
                String encoded = snapshotPendingTransactions.entrySet().stream()
                        .map(e -> e.getKey() + ":" + e.getValue().toString())
                        .collect(Collectors.joining(","));
                offset.put(SNAPSHOT_PENDING_TRANSACTIONS_KEY, encoded);
            }
            offset.put(SNAPSHOT_SCN_KEY, snapshotScn != null ? snapshotScn.isNull() ? null : snapshotScn.toString() : null);
            offset.put(YSTREAM_START_SCN_KEY, ystreamStartScn != null ? ystreamStartScn.isNull() ? null : ystreamStartScn.toString() : null);
            offset.put(YSTREAM_SERVER_CREATE, isCreateServer);
            offset.put(SourceInfo.POSITION_SCN_KEY, recoverPosition.getCommitScn().getScn());
            offset.put(SourceInfo.GROUP_LSN_KEY, recoverPosition.getLogPosition().getGroupLsn());
            offset.put(SourceInfo.GROUP_OFFSET_KEY, recoverPosition.getLogPosition().getGroupOffset());
            offset.put(SourceInfo.BATCH_ROW_ID_KEY, recoverPosition.getLogPosition().getBatchRowId());
            offset.put(SourceInfo.INSTANCE_ID_KEY, String.valueOf(recoverPosition.getLogPosition().getInstanceId()));

            return offset;
        }
        else {
            final Map<String, Object> offset = new HashMap<>();
            if (sourceInfo.getLcrPosition() != null) {
                // offset.put(SourceInfo.LCR_POSITION_KEY, sourceInfo.getLcrPosition());
                offset.put(SourceInfo.POSITION_SCN_KEY, sourceInfo.getPositionScn());
                offset.put(SourceInfo.GROUP_LSN_KEY, sourceInfo.getGroupLsn());
                offset.put(SourceInfo.GROUP_OFFSET_KEY, sourceInfo.getGroupOffset());
                offset.put(SourceInfo.INSTANCE_ID_KEY, sourceInfo.getInstanceId());
                offset.put(SourceInfo.BATCH_ROW_ID_KEY, sourceInfo.getBatchRowId());
            }
            else {
                // has not lcr position, use recoverPosition.
                final Scn scn = sourceInfo.getScn();
                offset.put(SourceInfo.SCN_KEY, scn != null ? scn.toString() : null);
                sourceInfo.getCommitScn().store(offset);
                offset.put(SourceInfo.POSITION_SCN_KEY, recoverPosition.getCommitScn().getScn());
                offset.put(SourceInfo.GROUP_LSN_KEY, recoverPosition.getLogPosition().getGroupLsn());
                offset.put(SourceInfo.GROUP_OFFSET_KEY, recoverPosition.getLogPosition().getGroupOffset());
                offset.put(SourceInfo.INSTANCE_ID_KEY, String.valueOf(recoverPosition.getLogPosition().getInstanceId()));
                offset.put(SourceInfo.BATCH_ROW_ID_KEY, recoverPosition.getLogPosition().getBatchRowId());
            }
            if (snapshotPendingTransactions != null && !snapshotPendingTransactions.isEmpty()) {
                String encoded = snapshotPendingTransactions.entrySet().stream()
                        .map(e -> e.getKey() + ":" + e.getValue().toString())
                        .collect(Collectors.joining(","));
                offset.put(SNAPSHOT_PENDING_TRANSACTIONS_KEY, encoded);
            }
            offset.put(SNAPSHOT_SCN_KEY, snapshotScn != null ? snapshotScn.isNull() ? null : snapshotScn.toString() : null);
            offset.put(YSTREAM_START_SCN_KEY, ystreamStartScn != null ? ystreamStartScn.isNull() ? null : ystreamStartScn.toString() : null);
            offset.put(YSTREAM_SERVER_CREATE, isCreateServer);
            return incrementalSnapshotContext.store(transactionContext.store(offset));
        }
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    public void setScn(Scn scn) {
        sourceInfo.setScn(scn);
    }

    public void setEventScn(Scn eventScn) {
        sourceInfo.setEventScn(eventScn);
    }

    public Scn getScn() {
        return sourceInfo.getScn();
    }

    public CommitScn getCommitScn() {
        return sourceInfo.getCommitScn();
    }

    public Scn getEventScn() {
        return sourceInfo.getEventScn();
    }

    public Scn getYstreamStartScn() {
        return ystreamStartScn;
    }

    public Position getRecoverPosition() {
        return recoverPosition;
    }

    public void setLcrPosition(Position lcrPosition) {
        sourceInfo.setLcrPosition(lcrPosition);
    }

    public Position getLcrPosition() {
        return sourceInfo.getLcrPosition();
    }

    public Scn getSnapshotScn() {
        return snapshotScn;
    }

    public Map<String, Scn> getSnapshotPendingTransactions() {
        return snapshotPendingTransactions;
    }

    public void setSnapshotPendingTransactions(Map<String, Scn> snapshotPendingTransactions) {
        this.snapshotPendingTransactions = snapshotPendingTransactions;
    }

    public void setTransactionId(String transactionId) {
        sourceInfo.setTransactionId(transactionId);
    }

    public void setUserName(String userName) {
        sourceInfo.setUserName(userName);
    }

    public void setSourceTime(Instant instant) {
        sourceInfo.setSourceTime(instant);
    }

    public void setTableId(TableId tableId) {
        sourceInfo.tableEvent(tableId);
    }

    public Integer getRedoThread() {
        return sourceInfo.getRedoThread();
    }

    public void setRedoThread(Integer redoThread) {
        sourceInfo.setRedoThread(redoThread);
    }

    public void setRsId(String rsId) {
        sourceInfo.setRsId(rsId);
    }

    public void setSsn(long ssn) {
        sourceInfo.setSsn(ssn);
    }

    public boolean isCreateServer() {
        return isCreateServer;
    }

    public void setCreateServer(boolean createServer) {
        isCreateServer = createServer;
    }

    @Override
    public boolean isInitialSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    @Override
    public void preSnapshotStart(boolean onDemand) {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OracleOffsetContext [scn=").append(getScn());

        if (sourceInfo.isSnapshot()) {
            sb.append(", snapshot=").append(sourceInfo.isSnapshot());
            sb.append(", snapshot_completed=").append(snapshotCompleted);
        }

        sb.append(", commit_scn=").append(sourceInfo.getCommitScn().toLoggableFormat());

        sb.append("]");

        return sb.toString();
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.tableEvent((TableId) tableId);
        sourceInfo.setSourceTime(timestamp);
    }

    public void tableEvent(TableId tableId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.tableEvent(tableId);
    }

    public void tableEvent(Set<TableId> tableIds, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.tableEvent(tableIds);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    /**
     * Helper method to resolve a {@link Scn} by key from the offset map.
     *
     * @param offset the offset map
     * @param key    the entry key, either {@link SourceInfo#SCN_KEY} or {@link SourceInfo#COMMIT_SCN_KEY}.
     * @return the {@link Scn} or null if not found
     */
    public static Scn getScnFromOffsetMapByKey(Map<String, ?> offset, String key) {
        Object scn = offset.get(key);
        if (scn instanceof String) {
            return Scn.valueOf((String) scn);
        }
        else if (scn != null) {
            return Scn.valueOf((Long) scn);
        }
        return null;
    }

    /**
     * Helper method to read the in-progress transaction map from the offset map.
     *
     * @param offset the offset map
     * @return the in-progress transaction map
     */
    public static Map<String, Scn> loadSnapshotPendingTransactions(Map<String, ?> offset) {
        Map<String, Scn> snapshotPendingTransactions = new HashMap<>();
        String encoded = (String) offset.get(SNAPSHOT_PENDING_TRANSACTIONS_KEY);
        if (encoded != null) {
            Arrays.stream(encoded.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .forEach(e -> {
                        String[] parts = e.split(":", 2);
                        String txid = parts[0];
                        Scn startScn = Scn.valueOf(parts[1]);
                        snapshotPendingTransactions.put(txid, startScn);
                    });
        }
        return snapshotPendingTransactions;
    }

    /**
     * Helper method to read the snapshot SCN from the offset map.
     *
     * @param offset the offset map
     * @return the snapshot SCN
     */
    public static Scn loadSnapshotScn(Map<String, ?> offset) {
        return getScnFromOffsetMapByKey(offset, SNAPSHOT_SCN_KEY);
    }

    public static Scn loadYstreamStartScn(Map<String, ?> offset) {
        return getScnFromOffsetMapByKey(offset, YSTREAM_START_SCN_KEY);
    }

    public static Position loadRecoverPosition(Map<String, ?> offset) {
        Object scn = offset.get(SourceInfo.POSITION_SCN_KEY);
        String instanceId = String.valueOf(offset.get(SourceInfo.INSTANCE_ID_KEY));
        Object groupLsn = offset.get(SourceInfo.GROUP_LSN_KEY);
        Object groupOffset = offset.get(SourceInfo.GROUP_OFFSET_KEY);
        Object batchRowId = offset.get(SourceInfo.BATCH_ROW_ID_KEY);

        byte instanceIdB;
        if (isDigit(instanceId)) {
            instanceIdB = Byte.parseByte(instanceId);
        }
        else {
            // 兼容旧数据 AAAAAAA=
            byte[] instanceIdBytes = Base64.getDecoder().decode(instanceId);
            instanceIdB = instanceIdBytes[0];
        }
        if (scn instanceof String) {
            return new Position(new SystemChangeNumber(Long.parseLong((String) scn)),
                    new LogPosition(instanceIdB, (Long) groupLsn, (Integer) groupOffset, (Integer) batchRowId));
        }
        else if (scn instanceof Long) {
            if (groupOffset instanceof Long) {
                return new Position(new SystemChangeNumber((Long) scn),
                        new LogPosition(instanceIdB, (Long) groupLsn, Math.toIntExact((Long) groupOffset), Math.toIntExact((Long) batchRowId)));
            }
            return new Position(new SystemChangeNumber((Long) scn), new LogPosition(instanceIdB, (Long) groupLsn, (Integer) groupOffset, (Integer) batchRowId));
        }
        else if (scn instanceof Integer) {
            return new Position(new SystemChangeNumber((Integer) scn), new LogPosition(instanceIdB, (Long) groupLsn, (Integer) groupOffset, (Integer) batchRowId));

        }
        else {
            return null;
        }
    }

    private static boolean isDigit(String s) {
        for (char c : s.toCharArray()) {
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        return true;
    }
}
