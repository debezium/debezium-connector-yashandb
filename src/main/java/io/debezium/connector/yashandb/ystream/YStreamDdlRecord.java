package io.debezium.connector.yashandb.ystream;

import com.sics.ystream.metadata.TableMetadata;
import com.sics.ystream.result.YstreamDdl;

public class YStreamDdlRecord extends YStreamRecord {
    private YstreamDdl ystreamDdl;

    public YStreamDdlRecord(YstreamDdl ystreamDdl, TableMetadata tableMetadata) {
        super(ystreamDdl, tableMetadata);
        this.ystreamDdl = ystreamDdl;
    }

    public YstreamDdl getYstreamDdl() {
        return ystreamDdl;
    }
}
