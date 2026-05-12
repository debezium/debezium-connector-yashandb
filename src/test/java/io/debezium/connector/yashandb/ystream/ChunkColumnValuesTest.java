/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sics.ystream.metadata.Column;
import com.sics.ystream.metadata.TableMetadata;
import com.sics.ystream.result.YstreamChunk;
import com.yashandb.jdbc.YasTypes;

import io.debezium.DebeziumException;

/**
 * Unit tests for {@link ChunkColumnValues}.
 */
class ChunkColumnValuesTest {

    private ChunkColumnValues chunkColumnValues;

    @BeforeEach
    void setUp() {
        chunkColumnValues = new ChunkColumnValues();
    }

    @Test
    void shouldBeEmptyWhenCreated() {
        assertThat(chunkColumnValues.isEmpty()).isTrue();
    }

    @Test
    void shouldNotBeEmptyAfterAddingChunk() {
        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.CLOB);

        YstreamChunk chunk = mock(YstreamChunk.class);
        when(chunk.getColumn()).thenReturn(column);

        chunkColumnValues.add(chunk);
        assertThat(chunkColumnValues.isEmpty()).isFalse();
    }

    @Test
    void shouldThrowExceptionWhenGetChunkTypeOnEmpty() {
        assertThatThrownBy(() -> chunkColumnValues.getChunkType())
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Unable to resolve chunk type since no chunks have yet been added");
    }

    @Test
    void shouldReturnChunkTypeAfterAddingChunk() {
        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.CLOB);

        YstreamChunk chunk = mock(YstreamChunk.class);
        when(chunk.getColumn()).thenReturn(column);

        chunkColumnValues.add(chunk);
        assertThat(chunkColumnValues.getChunkType()).isEqualTo(YasTypes.CLOB);
    }

    @Test
    void shouldReturnClobChunkType() {
        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.CLOB);

        YstreamChunk chunk = mock(YstreamChunk.class);
        when(chunk.getColumn()).thenReturn(column);

        chunkColumnValues.add(chunk);
        assertThat(chunkColumnValues.getChunkType()).isEqualTo(YasTypes.CLOB);
    }

    @Test
    void shouldReturnNclobChunkType() {
        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.NCLOB);

        YstreamChunk chunk = mock(YstreamChunk.class);
        when(chunk.getColumn()).thenReturn(column);

        chunkColumnValues.add(chunk);
        assertThat(chunkColumnValues.getChunkType()).isEqualTo(YasTypes.NCLOB);
    }

    @Test
    void shouldReturnBlobChunkType() {
        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.BLOB);

        YstreamChunk chunk = mock(YstreamChunk.class);
        when(chunk.getColumn()).thenReturn(column);

        chunkColumnValues.add(chunk);
        assertThat(chunkColumnValues.getChunkType()).isEqualTo(YasTypes.BLOB);
    }

    @Test
    void shouldReturnRawChunkType() {
        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.RAW);

        YstreamChunk chunk = mock(YstreamChunk.class);
        when(chunk.getColumn()).thenReturn(column);

        chunkColumnValues.add(chunk);
        assertThat(chunkColumnValues.getChunkType()).isEqualTo(YasTypes.RAW);
    }

    @Test
    void shouldReturnVarcharChunkType() {
        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.VARCHAR);

        YstreamChunk chunk = mock(YstreamChunk.class);
        when(chunk.getColumn()).thenReturn(column);

        chunkColumnValues.add(chunk);
        assertThat(chunkColumnValues.getChunkType()).isEqualTo(YasTypes.VARCHAR);
    }

    @Test
    void shouldReturnNvarcharChunkType() {
        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.NVARCHAR);

        YstreamChunk chunk = mock(YstreamChunk.class);
        when(chunk.getColumn()).thenReturn(column);

        chunkColumnValues.add(chunk);
        assertThat(chunkColumnValues.getChunkType()).isEqualTo(YasTypes.NVARCHAR);
    }

    @Test
    void shouldReturnSqlxmlChunkType() {
        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.SQLXML);

        YstreamChunk chunk = mock(YstreamChunk.class);
        when(chunk.getColumn()).thenReturn(column);

        chunkColumnValues.add(chunk);
        assertThat(chunkColumnValues.getChunkType()).isEqualTo(YasTypes.SQLXML);
    }

    @Test
    void shouldReturnNullStringValueWhenSizeIsZero() throws SQLException {
        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.CLOB);

        YstreamChunk chunk = mock(YstreamChunk.class);
        when(chunk.getColumn()).thenReturn(column);
        when(chunk.getBytes()).thenReturn(new byte[0]);
        when(chunk.getSize()).thenReturn(0);

        chunkColumnValues.add(chunk);
        assertThat(chunkColumnValues.getStringValue(mock(TableMetadata.class))).isNull();
    }

    @Test
    void shouldReturnNullByteArrayWhenSizeIsZero() throws SQLException {
        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.CLOB);

        YstreamChunk chunk = mock(YstreamChunk.class);
        when(chunk.getColumn()).thenReturn(column);
        when(chunk.getBytes()).thenReturn(new byte[0]);
        when(chunk.getSize()).thenReturn(0);

        chunkColumnValues.add(chunk);
        assertThat(chunkColumnValues.getByteArray()).isNull();
    }

    @Test
    void shouldReturnByteArrayAfterAddingChunks() throws SQLException {
        byte[] data1 = "Hello".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "World".getBytes(StandardCharsets.UTF_8);

        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.CLOB);

        YstreamChunk chunk1 = mock(YstreamChunk.class);
        when(chunk1.getColumn()).thenReturn(column);
        when(chunk1.getBytes()).thenReturn(data1);
        when(chunk1.getSize()).thenReturn(data1.length);

        YstreamChunk chunk2 = mock(YstreamChunk.class);
        when(chunk2.getColumn()).thenReturn(column);
        when(chunk2.getBytes()).thenReturn(data2);
        when(chunk2.getSize()).thenReturn(data2.length);

        chunkColumnValues.add(chunk1);
        chunkColumnValues.add(chunk2);

        byte[] result = chunkColumnValues.getByteArray();
        assertThat(result).isNotNull();
        assertThat(result).isEqualTo("HelloWorld".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldCombineMultipleChunksIntoByteArray() throws SQLException {
        byte[] data1 = new byte[]{ 1, 2, 3 };
        byte[] data2 = new byte[]{ 4, 5 };
        byte[] data3 = new byte[]{ 6, 7, 8, 9 };

        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.BLOB);

        YstreamChunk chunk1 = mock(YstreamChunk.class);
        when(chunk1.getColumn()).thenReturn(column);
        when(chunk1.getBytes()).thenReturn(data1);
        when(chunk1.getSize()).thenReturn(data1.length);

        YstreamChunk chunk2 = mock(YstreamChunk.class);
        when(chunk2.getColumn()).thenReturn(column);
        when(chunk2.getBytes()).thenReturn(data2);
        when(chunk2.getSize()).thenReturn(data2.length);

        YstreamChunk chunk3 = mock(YstreamChunk.class);
        when(chunk3.getColumn()).thenReturn(column);
        when(chunk3.getBytes()).thenReturn(data3);
        when(chunk3.getSize()).thenReturn(data3.length);

        chunkColumnValues.add(chunk1);
        chunkColumnValues.add(chunk2);
        chunkColumnValues.add(chunk3);

        byte[] result = chunkColumnValues.getByteArray();
        assertThat(result).isNotNull();
        assertThat(result).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    void shouldAccumulateSizeWhenAddingChunks() throws SQLException {
        byte[] data1 = new byte[10];
        byte[] data2 = new byte[20];

        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.CLOB);

        YstreamChunk chunk1 = mock(YstreamChunk.class);
        when(chunk1.getColumn()).thenReturn(column);
        when(chunk1.getBytes()).thenReturn(data1);
        when(chunk1.getSize()).thenReturn(10);

        YstreamChunk chunk2 = mock(YstreamChunk.class);
        when(chunk2.getColumn()).thenReturn(column);
        when(chunk2.getBytes()).thenReturn(data2);
        when(chunk2.getSize()).thenReturn(20);

        chunkColumnValues.add(chunk1);
        chunkColumnValues.add(chunk2);

        // size > 0 means getStringValue won't return null (it will call ChunkUtil.parseData)
        assertThat(chunkColumnValues.isEmpty()).isFalse();
    }

    @Test
    void shouldAccumulateMultipleChunksOfSameType() {
        Column column = mock(Column.class);
        when(column.getDataType()).thenReturn(YasTypes.CLOB);

        YstreamChunk chunk1 = mock(YstreamChunk.class);
        when(chunk1.getColumn()).thenReturn(column);

        YstreamChunk chunk2 = mock(YstreamChunk.class);
        when(chunk2.getColumn()).thenReturn(column);

        chunkColumnValues.add(chunk1);
        chunkColumnValues.add(chunk2);

        assertThat(chunkColumnValues.getChunkType()).isEqualTo(YasTypes.CLOB);
    }
}
