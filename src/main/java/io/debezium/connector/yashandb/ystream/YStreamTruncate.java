package io.debezium.connector.yashandb.ystream;

import com.sics.ystream.metadata.TableId;
import com.sics.ystream.result.DdlType;
import com.sics.ystream.result.ObjectType;
import com.sics.ystream.result.Position;
import com.sics.ystream.result.SystemChangeNumber;
import com.sics.ystream.result.YstreamMetadata;

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

    public int getSize() {
        return size;
    }

    public Position getPosition() {
        return position;
    }

    public int getSessionId() {
        return sessionId;
    }

    public TableId getTableId() {
        return tableId;
    }

    public TableId getOldTableId() {
        return oldTableId;
    }

    public boolean isRecover() {
        return isRecover;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public DdlType getDdlType() {
        return ddlType;
    }

    public ObjectType getObjectType() {
        return objectType;
    }

    public long getSsn() {
        return ssn;
    }

    public SystemChangeNumber getCurrentScn() {
        return currentScn;
    }

    public String getDdlText() {
        return ddlText;
    }

    public YstreamMetadata getYstreamMetadata() {
        return ystreamMetadata;
    }
}
