/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.sics.ystream.exception.YstreamSqlException;
import com.sics.ystream.metadata.TableMetadata;
import com.sics.ystream.result.YstreamChunk;
import com.sics.ystream.util.ChunkUtil;
import com.yashandb.jdbc.YasSQLXML;
import com.yashandb.jdbc.YasTypes;

import io.debezium.DebeziumException;

/**
 * A simple wrapper class around a collection of {@link YstreamChunk}s.
 */
public class ChunkColumnValues {

    private final List<YstreamChunk> values = new ArrayList<>();
    private long size = 0;

    /**
     * Gets the chunk type of the managed {@link YstreamChunk} instances.
     *
     * @return the chunk type of the values
     * @throws DebeziumException if the method is called before adding at least one ChunkColumnValue.
     */
    public int getChunkType() {
        if (values.isEmpty()) {
            throw new DebeziumException("Unable to resolve chunk type since no chunks have yet been added.");
        }
        return values.get(0).getColumn().getDataType();
    }

    /**
     * @return {@code true} if there are no values, {@code false} if at least one value has been added.
     */
    public boolean isEmpty() {
        return values.isEmpty();
    }

    /**
     * Adds a chunk column value instance to this collection.
     *
     * @param chunkColumnValue the chunk column value to be added
     */
    public void add(YstreamChunk chunkColumnValue) {
        size += calculateChunkSize(chunkColumnValue);
        values.add(chunkColumnValue);
    }

    /**
     * @return the chunk data as a string, may be {@code null} if the length of the data is zero.
     * @throws SQLException if there is a database exception accessing the raw chunk value
     */
    public String getStringValue(TableMetadata metadata) throws SQLException {
        if (size == 0) {
            return null;
        }
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            for (YstreamChunk value : values) {
                byteArrayOutputStream.write(value.getBytes());
            }
            return (String) ChunkUtil.parseData(
                    byteArrayOutputStream.toByteArray(),
                    values.get(0).getColumn(),
                    metadata.getCharset(), metadata.getNationalCharset());
        }
        catch (IOException e) {
            throw new SQLException(e);
        }
        catch (YstreamSqlException e) {
            throw new SQLException(e);
        }
    }

    /**
     * @return the chunk data as a byte array, may be {@code null} if the length of the data is zero.
     * @throws SQLException if there is a database exception accessing the raw chunk value
     */
    public byte[] getByteArray() throws SQLException {
        if (size == 0) {
            return null;
        }
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            for (YstreamChunk value : values) {
                byteArrayOutputStream.write(value.getBytes());
            }
            return byteArrayOutputStream.toByteArray();
        }
        catch (IOException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Calculates the size of the individual column chunk.
     *
     * @param chunkColumnValue a specific chunk of column data
     * @return the size of the column chunk data
     * @throws DebeziumException if there was a problem resolving the size of the column chunk data
     */
    private int calculateChunkSize(YstreamChunk chunkColumnValue) {
        switch (chunkColumnValue.getColumn().getDataType()) {
            case YasTypes.CLOB:
            case YasTypes.NCLOB:
            case YasTypes.SQLXML, YasTypes.VARCHAR,
                 YasTypes.NVARCHAR, YasTypes.RAW, YasTypes.BLOB:
                return chunkColumnValue.getSize();
            default:
                return 0;
        }
    }
}
