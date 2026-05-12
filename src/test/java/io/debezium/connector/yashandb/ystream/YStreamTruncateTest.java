/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import com.sics.ystream.result.DdlType;
import com.sics.ystream.result.LogPosition;
import com.sics.ystream.result.ObjectType;
import com.sics.ystream.result.Position;
import com.sics.ystream.result.SystemChangeNumber;
import com.sics.ystream.result.YstreamMetadata;

/**
 * Unit tests for {@link YStreamTruncate}.
 */
class YStreamTruncateTest {

    @Test
    void shouldReturnSize() {
        YStreamTruncate truncate = createTruncate(42);
        assertThat(truncate.getSize()).isEqualTo(42);
    }

    @Test
    void shouldReturnPosition() {
        Position position = mock(Position.class);
        YStreamTruncate truncate = createTruncateWithPosition(position);
        assertThat(truncate.getPosition()).isSameAs(position);
    }

    @Test
    void shouldReturnSessionId() {
        YStreamTruncate truncate = createTruncate(10);
        assertThat(truncate.getSessionId()).isEqualTo(12345);
    }

    @Test
    void shouldReturnTableId() {
        com.sics.ystream.metadata.TableId tableId = mock(com.sics.ystream.metadata.TableId.class);
        YStreamTruncate truncate = createTruncateWithTableId(tableId, null);
        assertThat(truncate.getTableId()).isSameAs(tableId);
    }

    @Test
    void shouldReturnOldTableId() {
        com.sics.ystream.metadata.TableId oldTableId = mock(com.sics.ystream.metadata.TableId.class);
        YStreamTruncate truncate = createTruncateWithTableId(null, oldTableId);
        assertThat(truncate.getOldTableId()).isSameAs(oldTableId);
    }

    @Test
    void shouldReturnIsRecoverTrue() {
        YStreamTruncate truncate = createTruncate(10, true);
        assertThat(truncate.isRecover()).isTrue();
    }

    @Test
    void shouldReturnIsRecoverFalse() {
        YStreamTruncate truncate = createTruncate(10, false);
        assertThat(truncate.isRecover()).isFalse();
    }

    @Test
    void shouldReturnTransactionId() {
        YStreamTruncate truncate = createTruncate(10);
        assertThat(truncate.getTransactionId()).isEqualTo(999);
    }

    @Test
    void shouldReturnDdlType() {
        DdlType ddlType = mock(DdlType.class);
        YStreamTruncate truncate = createTruncateWithDdlType(ddlType);
        assertThat(truncate.getDdlType()).isSameAs(ddlType);
    }

    @Test
    void shouldReturnObjectType() {
        YStreamTruncate truncate = createTruncate(10);
        assertThat(truncate.getObjectType()).isEqualTo(ObjectType.TABLE);
    }

    @Test
    void shouldReturnSsn() {
        YStreamTruncate truncate = createTruncate(10);
        assertThat(truncate.getSsn()).isEqualTo(7777L);
    }

    @Test
    void shouldReturnCurrentScn() {
        SystemChangeNumber scn = new SystemChangeNumber(50000L);
        YStreamTruncate truncate = createTruncateWithScn(scn);
        assertThat(truncate.getCurrentScn()).isSameAs(scn);
        assertThat(truncate.getCurrentScn().getScn()).isEqualTo(50000L);
    }

    @Test
    void shouldReturnDdlText() {
        YStreamTruncate truncate = createTruncate(10);
        assertThat(truncate.getDdlText()).isEqualTo("TRUNCATE TABLE test_table");
    }

    @Test
    void shouldReturnYstreamMetadata() {
        YstreamMetadata metadata = mock(YstreamMetadata.class);
        YStreamTruncate truncate = createTruncateWithMetadata(metadata);
        assertThat(truncate.getYstreamMetadata()).isSameAs(metadata);
    }

    @Test
    void shouldReturnAllFields() {
        Position position = new Position(
                new SystemChangeNumber(100L),
                new LogPosition((byte) 1, 200L, 3, 4));
        com.sics.ystream.metadata.TableId tableId = mock(com.sics.ystream.metadata.TableId.class);
        com.sics.ystream.metadata.TableId oldTableId = mock(com.sics.ystream.metadata.TableId.class);
        SystemChangeNumber scn = new SystemChangeNumber(300L);
        YstreamMetadata metadata = mock(YstreamMetadata.class);
        DdlType ddlType = mock(DdlType.class);

        YStreamTruncate truncate = new YStreamTruncate(
                50, position, 111, tableId, oldTableId, true,
                888, ddlType, ObjectType.TABLE, 5555L,
                scn, "TRUNCATE TABLE t", metadata);

        assertThat(truncate.getSize()).isEqualTo(50);
        assertThat(truncate.getPosition()).isSameAs(position);
        assertThat(truncate.getSessionId()).isEqualTo(111);
        assertThat(truncate.getTableId()).isSameAs(tableId);
        assertThat(truncate.getOldTableId()).isSameAs(oldTableId);
        assertThat(truncate.isRecover()).isTrue();
        assertThat(truncate.getTransactionId()).isEqualTo(888);
        assertThat(truncate.getDdlType()).isSameAs(ddlType);
        assertThat(truncate.getObjectType()).isEqualTo(ObjectType.TABLE);
        assertThat(truncate.getSsn()).isEqualTo(5555L);
        assertThat(truncate.getCurrentScn()).isSameAs(scn);
        assertThat(truncate.getDdlText()).isEqualTo("TRUNCATE TABLE t");
        assertThat(truncate.getYstreamMetadata()).isSameAs(metadata);
    }

    // Helper methods

    private YStreamTruncate createTruncate(int size) {
        Position position = new Position(
                new SystemChangeNumber(100L),
                new LogPosition((byte) 1, 200L, 3, 4));
        return new YStreamTruncate(
                size, position, 12345, null, null, false,
                999, mock(DdlType.class), ObjectType.TABLE, 7777L,
                new SystemChangeNumber(100L), "TRUNCATE TABLE test_table", null);
    }

    private YStreamTruncate createTruncate(int size, boolean isRecover) {
        Position position = new Position(
                new SystemChangeNumber(100L),
                new LogPosition((byte) 1, 200L, 3, 4));
        return new YStreamTruncate(
                size, position, 12345, null, null, isRecover,
                999, mock(DdlType.class), ObjectType.TABLE, 7777L,
                new SystemChangeNumber(100L), "TRUNCATE TABLE test_table", null);
    }

    private YStreamTruncate createTruncateWithPosition(Position position) {
        return new YStreamTruncate(
                10, position, 12345, null, null, false,
                999, mock(DdlType.class), ObjectType.TABLE, 7777L,
                new SystemChangeNumber(100L), "TRUNCATE TABLE test_table", null);
    }

    private YStreamTruncate createTruncateWithTableId(
                                                      com.sics.ystream.metadata.TableId tableId,
                                                      com.sics.ystream.metadata.TableId oldTableId) {
        Position position = new Position(
                new SystemChangeNumber(100L),
                new LogPosition((byte) 1, 200L, 3, 4));
        return new YStreamTruncate(
                10, position, 12345, tableId, oldTableId, false,
                999, mock(DdlType.class), ObjectType.TABLE, 7777L,
                new SystemChangeNumber(100L), "TRUNCATE TABLE test_table", null);
    }

    private YStreamTruncate createTruncateWithDdlType(DdlType ddlType) {
        Position position = new Position(
                new SystemChangeNumber(100L),
                new LogPosition((byte) 1, 200L, 3, 4));
        return new YStreamTruncate(
                10, position, 12345, null, null, false,
                999, ddlType, ObjectType.TABLE, 7777L,
                new SystemChangeNumber(100L), "TRUNCATE TABLE test_table", null);
    }

    private YStreamTruncate createTruncateWithScn(SystemChangeNumber scn) {
        Position position = new Position(
                new SystemChangeNumber(100L),
                new LogPosition((byte) 1, 200L, 3, 4));
        return new YStreamTruncate(
                10, position, 12345, null, null, false,
                999, mock(DdlType.class), ObjectType.TABLE, 7777L,
                scn, "TRUNCATE TABLE test_table", null);
    }

    private YStreamTruncate createTruncateWithMetadata(YstreamMetadata metadata) {
        Position position = new Position(
                new SystemChangeNumber(100L),
                new LogPosition((byte) 1, 200L, 3, 4));
        return new YStreamTruncate(
                10, position, 12345, null, null, false,
                999, mock(DdlType.class), ObjectType.TABLE, 7777L,
                new SystemChangeNumber(100L), "TRUNCATE TABLE test_table", metadata);
    }
}
