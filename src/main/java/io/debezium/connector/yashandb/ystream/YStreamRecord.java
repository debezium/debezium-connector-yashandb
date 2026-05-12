package io.debezium.connector.yashandb.ystream;

import com.sics.ystream.metadata.TableMetadata;
import com.sics.ystream.result.YstreamLcrInterface;

public class YStreamRecord {
    private final YstreamLcrInterface ystreamLcrInterface;
    private final TableMetadata tableMetadata;

    public YStreamRecord(YstreamLcrInterface ystreamLcrInterface, TableMetadata tableMetadata) {
        this.ystreamLcrInterface = ystreamLcrInterface;
        this.tableMetadata = tableMetadata;
    }

    public YstreamLcrInterface getYstreamLcrInterface() {
        return ystreamLcrInterface;
    }

    public TableMetadata getTableMetadata() {
        return tableMetadata;
    }
}
