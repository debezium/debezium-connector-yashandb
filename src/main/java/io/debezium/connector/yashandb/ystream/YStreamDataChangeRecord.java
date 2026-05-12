package io.debezium.connector.yashandb.ystream;

import com.sics.ystream.metadata.TableMetadata;
import com.sics.ystream.result.YstreamDml;

public class YStreamDataChangeRecord extends YStreamRecord {
    private YstreamDml ystreamDml;
    private YStreamTruncate yStreamTruncate;
    private final boolean isTruncateTable;

    public YStreamDataChangeRecord(YstreamDml ystreamDml, TableMetadata tableMetadata) {
        super(ystreamDml, tableMetadata);
        this.ystreamDml = ystreamDml;
        this.isTruncateTable = false;
    }

    public YStreamDataChangeRecord(YStreamTruncate yStreamTruncate) {
        super(null, null);
        this.isTruncateTable = true;
        this.yStreamTruncate = yStreamTruncate;
    }

    public YstreamDml getYstreamDml() {
        return ystreamDml;
    }

    public YStreamTruncate getyStreamTruncate() {
        return yStreamTruncate;
    }

    public boolean isTruncateTable() {
        return isTruncateTable;
    }
}
