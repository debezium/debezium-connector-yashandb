package io.debezium.connector.yashandb.ystream;

import com.sics.ystream.deserializer.Deserializer;
import com.sics.ystream.exception.YstreamException;
import com.sics.ystream.metadata.TableMetadata;
import com.sics.ystream.result.YstreamLcrInterface;

public class YStreamDeserializer implements Deserializer<YStreamRecord> {
    private static final long serialVersionUID = 1L;

    public YStreamDeserializer() {
    }

    public YStreamRecord deserialize(YstreamLcrInterface lcr, TableMetadata tableMetadata)
            throws YstreamException {
        return new YStreamRecord(lcr, tableMetadata);
    }
}
