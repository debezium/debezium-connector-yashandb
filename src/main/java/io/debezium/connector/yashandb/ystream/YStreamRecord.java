package io.debezium.connector.yashandb.ystream;

import com.sics.ystream.metadata.TableMetadata;
import com.sics.ystream.result.YstreamLcrInterface;

/**
 * A wrapper around a YStream LCR (Log Change Record) and its associated table metadata.
 */
public class YStreamRecord {
    private final YstreamLcrInterface ystreamLcrInterface;
    private final TableMetadata tableMetadata;

    /**
     * Creates a YStreamRecord with the given LCR and table metadata.
     *
     * @param ystreamLcrInterface the YStream LCR interface
     * @param tableMetadata the table metadata
     */
    public YStreamRecord(YstreamLcrInterface ystreamLcrInterface, TableMetadata tableMetadata) {
        this.ystreamLcrInterface = ystreamLcrInterface;
        this.tableMetadata = tableMetadata;
    }

    /**
     * @return the YStream LCR interface
     */
    public YstreamLcrInterface getYstreamLcrInterface() {
        return ystreamLcrInterface;
    }

    /**
     * @return the table metadata
     */
    public TableMetadata getTableMetadata() {
        return tableMetadata;
    }
}
