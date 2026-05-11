package io.debezium.connector.yashandb.ystream;

import com.sics.ystream.metadata.TableMetadata;
import com.sics.ystream.result.YstreamDdl;

/**
 * A {@link YStreamRecord} that represents a DDL (Data Definition Language) change event.
 */
public class YStreamDdlRecord extends YStreamRecord {
    private YstreamDdl ystreamDdl;

    /**
     * Creates a YStreamDdlRecord with the given DDL and table metadata.
     *
     * @param ystreamDdl the YStream DDL
     * @param tableMetadata the table metadata
     */
    public YStreamDdlRecord(YstreamDdl ystreamDdl, TableMetadata tableMetadata) {
        super(ystreamDdl, tableMetadata);
        this.ystreamDdl = ystreamDdl;
    }

    /**
     * @return the YStream DDL
     */
    public YstreamDdl getYstreamDdl() {
        return ystreamDdl;
    }
}
