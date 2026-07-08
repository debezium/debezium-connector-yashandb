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

import com.sics.ystream.result.LogPosition;
import com.sics.ystream.result.Position;
import com.sics.ystream.result.SystemChangeNumber;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Tracks the offset context for the YashanDB connector during both snapshot and streaming operations, maintaining SCN positions, transaction state, and incremental snapshot context.
 */
public class YashanDbOffsetContext extends CommonOffsetContext<SourceInfo> {

    public static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";
    public static final String SNAPSHOT_PENDING_TRANSACTIONS_KEY = "snapshot_pending_tx";
    public static final String SNAPSHOT_SCN_KEY = "snapshot_scn";

    private final Schema sourceInfoSchema;

    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;

    /**
     * SCN that was used for the initial consistent snapshot. This value is refreshed with each event.
     * Initially, this value differs from sourceInfo.getLcrPosition by 1 to ensure data consistency
     * between full snapshot and incremental (CDC) data.
     * <p>
     * We keep track of this field because it's a cutoff for emitting DDL statements,
     * in case we start mining _before_ the snapshot SCN to cover transactions that were
     * ongoing at the time the snapshot was taken.
     */
    private Scn snapshotScn;

    /**
     * Map of (txid, start SCN) for all transactions in progress at the time the
     * snapshot was taken.
     */
    private Map<String, Scn> snapshotPendingTransactions;

    /**
     * Whether a snapshot has been completed or not.
     */
    private boolean snapshotCompleted;

    /**
     * Creates a YashanDbOffsetContext instance initialized with the given parameters for tracking offset state.
     *
     * @param connectorConfig the connector configuration
     * @param snapshotScn the snapshot SCN
     * @param recoverPosition the recover position
     * @param snapshotPendingTransactions the pending transactions map
     * @param snapshot whether a snapshot is in progress
     * @param snapshotCompleted whether the snapshot is completed
     * @param transactionContext the transaction context
     * @param incrementalSnapshotContext the incremental snapshot context
     */
    public YashanDbOffsetContext(YashanDbConnectorConfig connectorConfig,
                                 Scn snapshotScn, Position recoverPosition, Map<String, Scn> snapshotPendingTransactions,
                                 boolean snapshot, boolean snapshotCompleted, TransactionContext transactionContext,
                                 IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        super(new SourceInfo(connectorConfig));
        sourceInfo.setLcrPosition(recoverPosition);
        sourceInfoSchema = sourceInfo.schema();

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

    /**
     * Builder for constructing YashanDbOffsetContext instances with a fluent API.
     */
    public static class Builder {

        private YashanDbConnectorConfig connectorConfig;
        private boolean snapshot;
        private boolean snapshotCompleted;
        private TransactionContext transactionContext;
        private IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
        private Map<String, Scn> snapshotPendingTransactions;
        private Scn snapshotScn;
        private Position recoverPosition;

        /**
         * Sets the connector configuration (logical name).
         *
         * @param connectorConfig the connector configuration
         *
         * @return this builder for method chaining
         */
        public Builder logicalName(YashanDbConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
            return this;
        }

        /**
         * Sets the recover position.
         *
         * @param recoverPosition the recover position
         *
         * @return this builder for method chaining
         */
        public Builder recoverPosition(Position recoverPosition) {
            this.recoverPosition = recoverPosition;
            return this;
        }

        /**
         * Sets the snapshot flag.
         *
         * @param snapshot the snapshot flag
         *
         * @return this builder for method chaining
         */
        public Builder snapshot(boolean snapshot) {
            this.snapshot = snapshot;
            return this;
        }

        /**
         * Sets the snapshot completion flag.
         *
         * @param snapshotCompleted the snapshot completed flag
         *
         * @return this builder for method chaining
         */
        public Builder snapshotCompleted(boolean snapshotCompleted) {
            this.snapshotCompleted = snapshotCompleted;
            return this;
        }

        /**
         * Sets the transaction context.
         *
         * @param transactionContext the transaction context
         *
         * @return this builder for method chaining
         */
        public Builder transactionContext(TransactionContext transactionContext) {
            this.transactionContext = transactionContext;
            return this;
        }

        /**
         * Sets the incremental snapshot context.
         *
         * @param incrementalSnapshotContext the incremental snapshot context
         *
         * @return this builder for method chaining
         */
        public Builder incrementalSnapshotContext(IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
            this.incrementalSnapshotContext = incrementalSnapshotContext;
            return this;
        }

        /**
         * Sets the snapshot pending transactions map.
         *
         * @param snapshotPendingTransactions the pending transaction map
         *
         * @return this builder for method chaining
         */
        public Builder snapshotPendingTransactions(Map<String, Scn> snapshotPendingTransactions) {
            this.snapshotPendingTransactions = snapshotPendingTransactions;
            return this;
        }

        /**
         * Sets the snapshot SCN.
         *
         * @param scn the snapshot SCN
         *
         * @return this builder for method chaining
         */
        public Builder snapshotScn(Scn scn) {
            this.snapshotScn = scn;
            return this;
        }

        /**
         * Builds and returns a YashanDbOffsetContext instance from the configured builder parameters.
         *
         * @return the built YashanDbOffsetContext instance
         */
        public YashanDbOffsetContext build() {
            return new YashanDbOffsetContext(connectorConfig, snapshotScn, recoverPosition, snapshotPendingTransactions, snapshot,
                    snapshotCompleted, transactionContext,
                    incrementalSnapshotContext);
        }
    }

    /**
     * Creates a new builder for constructing YashanDbOffsetContext instances.
     *
     * @return a new builder instance
     */
    public static Builder create() {
        return new Builder();
    }

    /**
     * Returns the current offset map for both snapshot and streaming states.
     *
     * @return the offset map
     */
    @Override
    public Map<String, ?> getOffset() {
        Position lcrPosition = sourceInfo.getLcrPosition();
        if (getSnapshot().isPresent()) {
            Map<String, Object> offset = new HashMap<>();

            offset.put(SourceInfo.SNAPSHOT_KEY, getSnapshot().get().toString());
            offset.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);

            if (snapshotPendingTransactions != null && !snapshotPendingTransactions.isEmpty()) {
                String encoded = snapshotPendingTransactions.entrySet().stream()
                        .map(e -> e.getKey() + ":" + e.getValue().toString())
                        .collect(Collectors.joining(","));
                offset.put(SNAPSHOT_PENDING_TRANSACTIONS_KEY, encoded);
            }
            offset.put(SNAPSHOT_SCN_KEY, snapshotScn != null ? snapshotScn.isNull() ? null : snapshotScn.toString() : null);
            offset.put(SourceInfo.POSITION_SCN_KEY, lcrPosition.getCommitScn().getScn());
            offset.put(SourceInfo.GROUP_LSN_KEY, lcrPosition.getLogPosition().getGroupLsn());
            offset.put(SourceInfo.GROUP_OFFSET_KEY, lcrPosition.getLogPosition().getGroupOffset());
            offset.put(SourceInfo.BATCH_ROW_ID_KEY, lcrPosition.getLogPosition().getBatchRowId());
            offset.put(SourceInfo.INSTANCE_ID_KEY, String.valueOf(lcrPosition.getLogPosition().getInstanceId()));

            return offset;
        }
        else {
            final Map<String, Object> offset = new HashMap<>();
            offset.put(SourceInfo.POSITION_SCN_KEY, lcrPosition.getCommitScn().getScn());
            offset.put(SourceInfo.GROUP_LSN_KEY, lcrPosition.getLogPosition().getGroupLsn());
            offset.put(SourceInfo.GROUP_OFFSET_KEY, lcrPosition.getLogPosition().getGroupOffset());
            offset.put(SourceInfo.BATCH_ROW_ID_KEY, lcrPosition.getLogPosition().getBatchRowId());
            offset.put(SourceInfo.INSTANCE_ID_KEY, String.valueOf(lcrPosition.getLogPosition().getInstanceId()));
            if (snapshotPendingTransactions != null && !snapshotPendingTransactions.isEmpty()) {
                String encoded = snapshotPendingTransactions.entrySet().stream()
                        .map(e -> e.getKey() + ":" + e.getValue().toString())
                        .collect(Collectors.joining(","));
                offset.put(SNAPSHOT_PENDING_TRANSACTIONS_KEY, encoded);
            }
            offset.put(SNAPSHOT_SCN_KEY, snapshotScn != null ? snapshotScn.isNull() ? null : snapshotScn.toString() : null);
            return incrementalSnapshotContext.store(transactionContext.store(offset));
        }
    }

    /**
     * Returns the schema of the source info.
     *
     * @return the source info schema
     */
    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    /**
     * Returns the recover position used to resume streaming.
     *
     * @return the recover position
     */
    public Position getRecoverPosition() {
        return sourceInfo.getLcrPosition();
    }

    /**
     * Sets the LCR position for the current event.
     *
     * @param lcrPosition the LCR position to set
     */
    public void setLcrPosition(Position lcrPosition) {
        // reset snapshotScn, because of will used for blocking snapshot
        snapshotScn = Scn.valueOf(lcrPosition.getCommitScn().getScn());
        sourceInfo.setLcrPosition(lcrPosition);
    }

    /**
     * Returns the current LCR position.
     *
     * @return the LCR position
     */
    public Position getLcrPosition() {
        return sourceInfo.getLcrPosition();
    }

    /**
     * Returns the SCN that was used for the initial consistent snapshot.
     *
     * @return the snapshot SCN
     */
    public Scn getSnapshotScn() {
        return snapshotScn;
    }

    /**
     * Returns the map of in-progress transactions at snapshot time.
     *
     * @return the snapshot pending transactions map
     */
    public Map<String, Scn> getSnapshotPendingTransactions() {
        return snapshotPendingTransactions;
    }

    /**
     * Sets the map of in-progress transactions at snapshot time.
     *
     * @param snapshotPendingTransactions the pending transaction map
     */
    public void setSnapshotPendingTransactions(Map<String, Scn> snapshotPendingTransactions) {
        this.snapshotPendingTransactions = snapshotPendingTransactions;
    }

    /**
     * Sets the transaction identifier for the current event.
     *
     * @param transactionId the transaction identifier to set
     */
    public void setTransactionId(String transactionId) {
        sourceInfo.setTransactionId(transactionId);
    }

    /**
     * Sets the source time for the current event.
     *
     * @param instant the source time instant to set
     */
    public void setSourceTime(Instant instant) {
        sourceInfo.setSourceTime(instant);
    }

    /**
     * Sets the current table event to the given table ID.
     *
     * @param tableId the table identifier
     */
    public void setTableId(TableId tableId) {
        sourceInfo.tableEvent(tableId);
    }

    /**
     * Returns whether an initial snapshot is currently running.
     *
     * @return true if a snapshot is in progress, false otherwise
     */
    @Override
    public boolean isInitialSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    /**
     * Called before the snapshot starts to initialize the snapshot state.
     *
     * @param onDemand whether the snapshot is triggered on demand
     */
    @Override
    public void preSnapshotStart(boolean onDemand) {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        snapshotCompleted = false;
    }

    /**
     * Called before the snapshot completion to mark the snapshot as complete.
     */
    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    /**
     * Returns a string representation of this offset context.
     *
     * @return the string representation
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("YashanDbOffsetContext [positionScn=").append(sourceInfo.getPositionScn());

        if (sourceInfo.isSnapshot()) {
            sb.append(", snapshot=").append(sourceInfo.isSnapshot());
            sb.append(", snapshot_completed=").append(snapshotCompleted);
        }

        sb.append("]");

        return sb.toString();
    }

    /**
     * Records an event for the given data collection and timestamp.
     *
     * @param tableId the table identifier
     * @param timestamp the event timestamp
     */
    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.tableEvent((TableId) tableId);
        sourceInfo.setSourceTime(timestamp);
    }

    /**
     * Records a table event with the given table ID and source timestamp.
     *
     * @param tableId the table identifier
     *
     * @param timestamp the event timestamp
     */
    public void tableEvent(TableId tableId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.tableEvent(tableId);
    }

    /**
     * Records multiple table events with the given table IDs and source timestamp.
     *
     * @param tableIds the set of table identifiers
     *
     * @param timestamp the event timestamp
     */
    public void tableEvent(Set<TableId> tableIds, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.tableEvent(tableIds);
    }

    /**
     * Returns the transaction context.
     *
     * @return the transaction context
     */
    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    /**
     * Returns the incremental snapshot context.
     *
     * @return the incremental snapshot context
     */
    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    /**
     * Helper method to resolve a {@link Scn} by key from the offset map.
     *
     * @param offset the offset map
     * @param key    the entry key in the offset map.
     * @return the {@link Scn} or null if not found
     */
    public static Scn getScnFromOffsetMapByKey(Map<String, ?> offset, String key) {
        Object scn = offset.get(key);
        if (scn instanceof String str) {
            return Scn.valueOf(str);
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

    /**
     * Loads the recover position from the offset map.
     *
     * @param offset the offset map
     *
     * @return the recovered Position, or null if not found
     */
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
            // Backward compatibility with legacy offset data (e.g., Base64-encoded instance ID like "AAAAAAA=")
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
