package io.debezium.connector.yashandb.ystream;

import com.sics.ystream.metadata.TableMetadata;
import com.sics.ystream.result.YstreamDml;

/**
 * A {@link YStreamRecord} that represents a DML (Data Manipulation Language) change event
 * or a truncate event.
 */
public class YStreamDataChangeRecord extends YStreamRecord {
    private YstreamDml ystreamDml;
    private YStreamTruncate yStreamTruncate;
    private final boolean isTruncateTable;

    /**
     * Creates a YStreamDataChangeRecord for a DML event.
     *
     * @param ystreamDml the YStream DML
     * @param tableMetadata the table metadata
     */
    public YStreamDataChangeRecord(YstreamDml ystreamDml, TableMetadata tableMetadata) {
        super(ystreamDml, tableMetadata);
        this.ystreamDml = ystreamDml;
        this.isTruncateTable = false;
    }

    /**
     * Creates a YStreamDataChangeRecord for a truncate event.
     *
     * @param yStreamTruncate the YStream truncate data
     */
    public YStreamDataChangeRecord(YStreamTruncate yStreamTruncate) {
        super(null, null);
        this.isTruncateTable = true;
        this.yStreamTruncate = yStreamTruncate;
    }

    /**
     * @return the YStream DML
     */
    public YstreamDml getYstreamDml() {
        return ystreamDml;
    }

    /**
     * @return the YStream truncate data
     */
    public YStreamTruncate getyStreamTruncate() {
        return yStreamTruncate;
    }

    /**
     * @return true if this record represents a truncate operation, false otherwise
     */
    public boolean isTruncateTable() {
        return isTruncateTable;
    }
}
