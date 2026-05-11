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
    public static final String SCN_KEY = "scn";
    public static final String EVENT_SCN_KEY = "scn";
    public static final String COMMIT_SCN_KEY = "commit_scn";
    public static final String LCR_POSITION_KEY = "lcr_position";
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String USERNAME_KEY = "user_name";

    public static final String POSITION_SCN_KEY = "position_scn";
    public static final String INSTANCE_ID_KEY = "instance_id";
    public static final String GROUP_LSN_KEY = "group_lsn";
    public static final String GROUP_OFFSET_KEY = "group_offset";
    public static final String BATCH_ROW_ID_KEY = "batch_row_id";

    private Scn scn;
    private CommitScn commitScn;
    private Scn eventScn;
    private Position lcrPosition;
    private String transactionId;
    private String userName;
    private Instant sourceTime;
    private Set<TableId> tableIds;
    private Integer redoThread;
    private String rsId;
    private long ssn;

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
    }

    /**
     * Returns the current system change number.
     *
     * @return the current SCN
     */
    public Scn getScn() {
        return scn;
    }

    /**
     * Returns the commit SCN tracking the high-watermark across redo threads.
     *
     * @return the commit SCN
     */
    public CommitScn getCommitScn() {
        return commitScn;
    }

    /**
     * Returns the SCN for the current change event.
     *
     * @return the event SCN
     */
    public Scn getEventScn() {
        return eventScn;
    }

    /**
     * Sets the current system change number.
     *
     * @param scn the SCN to set
     */
    public void setScn(Scn scn) {
        this.scn = scn;
    }

    /**
     * Sets the commit SCN for redo thread tracking.
     *
     * @param commitScn the commit SCN to set
     */
    public void setCommitScn(CommitScn commitScn) {
        this.commitScn = commitScn;
    }

    /**
     * Sets the SCN for the current change event.
     *
     * @param eventScn the event SCN to set
     */
    public void setEventScn(Scn eventScn) {
        this.eventScn = eventScn;
    }

    /**
     * Constructs and returns the current YStream position.
     *
     * @return the LCR position
     */
    public Position getLcrPosition() {
        return new Position(new SystemChangeNumber(positionScn), new LogPosition(Byte.parseByte(instanceId), groupLsn, groupOffset, batchRowId));
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
     * Returns the database user name associated with the current event.
     *
     * @return the user name
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Sets the database user name for the current event.
     *
     * @param userName the user name to set
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * Returns the redo record segment identifier for the current event.
     *
     * @return the RS ID
     */
    public String getRsId() {
        return rsId;
    }

    /**
     * Sets the redo record segment identifier for the current event.
     *
     * @param rsId the RS ID to set
     */
    public void setRsId(String rsId) {
        this.rsId = rsId;
    }

    /**
     * Returns the SQL sequence number for the current event.
     *
     * @return the SSN
     */
    public long getSsn() {
        return ssn;
    }

    /**
     * Sets the SQL sequence number for the current event.
     *
     * @param ssn the SSN to set
     */
    public void setSsn(long ssn) {
        this.ssn = ssn;
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
     * Returns the redo thread number for the current event.
     *
     * @return the redo thread number
     */
    public Integer getRedoThread() {
        return redoThread;
    }

    /**
     * Sets the redo thread number for the current event.
     *
     * @param redoThread the redo thread number to set
     */
    public void setRedoThread(Integer redoThread) {
        this.redoThread = redoThread;
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
     *
     * @return the database catalog name, or null if no tables are set
     */
    @Override
    protected String database() {
        return (tableIds != null) ? tableIds.iterator().next().catalog() : null;
    }
}
