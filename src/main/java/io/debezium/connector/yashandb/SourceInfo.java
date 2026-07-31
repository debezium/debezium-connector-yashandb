/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.sics.ystream.result.LogPosition;
import com.sics.ystream.result.Position;
import com.sics.ystream.result.SystemChangeNumber;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

/**
 * Captures source information for YashanDB change events, including SCN positions,
 * transaction metadata, table identifiers, and YStream position tracking.
 * <p>
 * This class is not thread-safe and should be accessed by a single thread at a time.
 *
 * @author Debezium Authors
 */
@NotThreadSafe
public class SourceInfo extends BaseSourceInfo {

    public static final String TXID_KEY = "txId";
    public static final String USERNAME_KEY = "user_name";

    public static final String POSITION_SCN_KEY = "position_scn";
    public static final String INSTANCE_ID_KEY = "instance_id";
    public static final String GROUP_LSN_KEY = "group_lsn";
    public static final String GROUP_OFFSET_KEY = "group_offset";
    public static final String BATCH_ROW_ID_KEY = "batch_row_id";

    private String transactionId;
    private Instant sourceTime;
    private Set<TableId> tableIds;
    private final String databaseName;

    // YStream position
    private long positionScn;
    private String instanceId;
    private long groupLsn;
    private int groupOffset;
    private int batchRowId;

    /**
     * Creates a SourceInfo instance initialized with the given connector configuration.
     *
     * @param connectorConfig the connector configuration
     */
    protected SourceInfo(YashanDbConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.databaseName = connectorConfig.getDatabaseName();
    }

    /**
     * Constructs and returns the current YStream position.
     *
     * @return the LCR position
     */
    public Position getLcrPosition() {
        return new Position(new SystemChangeNumber(positionScn), new LogPosition(instanceId == null ? 0 : Byte.parseByte(instanceId), groupLsn, groupOffset, batchRowId));
    }

    /**
     * Returns the SCN component of the YStream position.
     *
     * @return the position SCN
     */
    public long getPositionScn() {
        return positionScn;
    }

    /**
     * Returns the YashanDB instance identifier.
     *
     * @return the instance ID
     */
    public String getInstanceId() {
        return instanceId;
    }

    /**
     * Returns the group LSN (Log Sequence Number) from the YStream position.
     *
     * @return the group LSN
     */
    public long getGroupLsn() {
        return groupLsn;
    }

    /**
     * Returns the group offset within the YStream log group.
     *
     * @return the group offset
     */
    public int getGroupOffset() {
        return groupOffset;
    }

    /**
     * Returns the batch row identifier within the current YStream batch.
     *
     * @return the batch row ID
     */
    public int getBatchRowId() {
        return batchRowId;
    }

    /**
     * Sets the YStream position and extracts its components into individual fields.
     *
     * @param lcrPosition the LCR position to set
     */
    public void setLcrPosition(Position lcrPosition) {
        if (lcrPosition == null) {
            return;
        }
        this.positionScn = lcrPosition.getCommitScn().getScn();
        this.batchRowId = lcrPosition.getLogPosition().getBatchRowId();
        this.groupLsn = lcrPosition.getLogPosition().getGroupLsn();
        this.instanceId = String.valueOf(lcrPosition.getLogPosition().getInstanceId());
        this.groupOffset = lcrPosition.getLogPosition().getGroupOffset();
    }

    /**
     * Returns the transaction identifier for the current event.
     *
     * @return the transaction ID
     */
    public String getTransactionId() {
        return transactionId;
    }

    /**
     * Sets the transaction identifier for the current event.
     *
     * @param transactionId the transaction ID to set
     */
    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * Returns the source timestamp of the current event.
     *
     * @return the source time instant
     */
    public Instant getSourceTime() {
        return sourceTime;
    }

    /**
     * Sets the source timestamp for the current event.
     *
     * @param sourceTime the source time instant to set
     */
    public void setSourceTime(Instant sourceTime) {
        this.sourceTime = sourceTime;
    }

    /**
     * Returns the comma-separated list of distinct schema names for the current table event.
     *
     * @return the schema names, or null if no tables are set
     */
    public String tableSchema() {
        return (tableIds == null || tableIds.isEmpty()) ? null
                : tableIds.stream()
                        .filter(Objects::nonNull)
                        .map(TableId::schema)
                        .distinct()
                        .collect(Collectors.joining(","));
    }

    /**
     * Returns the comma-separated list of table names for the current table event.
     *
     * @return the table names, or null if no tables are set
     */
    public String table() {
        return (tableIds == null || tableIds.isEmpty()) ? null
                : tableIds.stream()
                        .filter(Objects::nonNull)
                        .map(TableId::table)
                        .collect(Collectors.joining(","));
    }

    /**
     * Sets the current table event to the given set of table identifiers.
     *
     * @param tableIds the set of table identifiers
     */
    public void tableEvent(Set<TableId> tableIds) {
        this.tableIds = new LinkedHashSet<>(tableIds);
    }

    /**
     * Sets the current table event to the given table identifier.
     *
     * @param tableId the table identifier
     */
    public void tableEvent(TableId tableId) {
        this.tableIds = Collections.singleton(tableId);
    }

    /**
     * Returns the timestamp of the current event.
     *
     * @return the source time instant
     */
    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    /**
     * Returns the database catalog name for the current table event.
     * Uses the configured database name from the connector configuration,
     * ensuring the {@code db} field in the source struct is always populated
     * even when the JDBC driver does not report catalog information.
     *
     * @return the database catalog name
     */
    @Override
    protected String database() {
        return databaseName;
    }
}
