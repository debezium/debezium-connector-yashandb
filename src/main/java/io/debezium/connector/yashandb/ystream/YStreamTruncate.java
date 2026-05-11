package io.debezium.connector.yashandb.ystream;

import com.sics.ystream.metadata.TableId;
import com.sics.ystream.result.DdlType;
import com.sics.ystream.result.ObjectType;
import com.sics.ystream.result.Position;
import com.sics.ystream.result.SystemChangeNumber;
import com.sics.ystream.result.YstreamMetadata;

/**
 * Represents a truncate event from YashanDB's YStream, encapsulating all metadata
 * associated with a TRUNCATE DDL operation.
 */
public class YStreamTruncate {
    private final int size;
    private final Position position;
    private final int sessionId;
    private final TableId tableId;
    private final TableId oldTableId;
    private final boolean isRecover;
    private final int transactionId;
    private final DdlType ddlType;
    private final ObjectType objectType;
    private final long ssn;
    private final SystemChangeNumber currentScn;
    private final String ddlText;
    private final YstreamMetadata ystreamMetadata;

    /**
     * Creates a YStreamTruncate instance with the given truncate event data.
     *
     * @param size the size of the truncate operation
     * @param position the LCR position of this event
     * @param sessionId the database session ID
     * @param tableId the target table ID
     * @param oldTableId the original table ID before the truncate
     * @param isRecover whether this is a recovery operation
     * @param transactionId the transaction ID
     * @param ddlType the DDL type
     * @param objectType the object type
     * @param ssn the system serial number
     * @param currentScn the current system change number
     * @param ddlText the DDL text of the truncate operation
     * @param ystreamMetadata the YStream metadata
     */
    public YStreamTruncate(int size, Position position,
                           int sessionId, TableId tableId, TableId oldTableId,
                           boolean isRecover, int transactionId, DdlType ddlType,
                           ObjectType objectType, long ssn,
                           SystemChangeNumber currentScn,
                           String ddlText,
                           YstreamMetadata ystreamMetadata) {
        this.size = size;
        this.position = position;
        this.sessionId = sessionId;
        this.tableId = tableId;
        this.oldTableId = oldTableId;
        this.isRecover = isRecover;
        this.transactionId = transactionId;
        this.ddlType = ddlType;
        this.objectType = objectType;
        this.ssn = ssn;
        this.currentScn = currentScn;
        this.ddlText = ddlText;
        this.ystreamMetadata = ystreamMetadata;
    }

    /**
     * @return the size of the truncate operation
     */
    public int getSize() {
        return size;
    }

    /**
     * @return the LCR position of this event
     */
    public Position getPosition() {
        return position;
    }

    /**
     * @return the database session ID
     */
    public int getSessionId() {
        return sessionId;
    }

    /**
     * @return the target table ID
     */
    public TableId getTableId() {
        return tableId;
    }

    /**
     * @return the original table ID before the truncate
     */
    public TableId getOldTableId() {
        return oldTableId;
    }

    /**
     * @return whether this is a recovery operation
     */
    public boolean isRecover() {
        return isRecover;
    }

    /**
     * @return the transaction ID
     */
    public int getTransactionId() {
        return transactionId;
    }

    /**
     * @return the DDL type
     */
    public DdlType getDdlType() {
        return ddlType;
    }

    /**
     * @return the object type
     */
    public ObjectType getObjectType() {
        return objectType;
    }

    /**
     * @return the system serial number
     */
    public long getSsn() {
        return ssn;
    }

    /**
     * @return the current system change number
     */
    public SystemChangeNumber getCurrentScn() {
        return currentScn;
    }

    /**
     * @return the DDL text of the truncate operation
     */
    public String getDdlText() {
        return ddlText;
    }

    /**
     * @return the YStream metadata
     */
    public YstreamMetadata getYstreamMetadata() {
        return ystreamMetadata;
    }
}
