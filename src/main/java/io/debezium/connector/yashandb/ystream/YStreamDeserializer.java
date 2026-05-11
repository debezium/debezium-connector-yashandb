package io.debezium.connector.yashandb.ystream;

import com.sics.ystream.deserializer.Deserializer;
import com.sics.ystream.exception.YstreamException;
import com.sics.ystream.metadata.TableMetadata;
import com.sics.ystream.result.YstreamLcrInterface;

/**
 * A {@link Deserializer} implementation that converts YStream LCR data into {@link YStreamRecord} instances.
 */
public class YStreamDeserializer implements Deserializer<YStreamRecord> {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new YStreamDeserializer instance.
     */
    public YStreamDeserializer() {
    }

    /**
     * Deserializes a YStream LCR into a {@link YStreamRecord}.
     *
     * @param lcr the YStream LCR interface containing the change data
     * @param tableMetadata the table metadata for the LCR
     * @return a YStreamRecord wrapping the LCR and table metadata
     * @throws YstreamException if deserialization fails
     */
    public YStreamRecord deserialize(YstreamLcrInterface lcr, TableMetadata tableMetadata)
            throws YstreamException {
        return new YStreamRecord(lcr, tableMetadata);
    }
}
