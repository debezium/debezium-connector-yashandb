/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.sics.ystream.result.Position;

/**
 * Unit tests for static methods in {@link YashanDbOffsetContext}.
 */
class YashanDbOffsetContextTest {

    @Test
    void shouldResolveScnFromStringFromOffsetMap() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.SCN_KEY, "12345");
        Scn scn = YashanDbOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        assertThat(scn).isNotNull();
        assertThat(scn.longValue()).isEqualTo(12345);
    }

    @Test
    void shouldResolveScnFromLongFromOffsetMap() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.SCN_KEY, 99999L);
        Scn scn = YashanDbOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        assertThat(scn).isNotNull();
        assertThat(scn.longValue()).isEqualTo(99999);
    }

    @Test
    void shouldReturnNullWhenScnKeyMissing() {
        Map<String, Object> offset = new HashMap<>();
        Scn scn = YashanDbOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        assertThat(scn).isNull();
    }

    @Test
    void shouldResolveCommitScnFromOffsetMap() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.COMMIT_SCN_KEY, "54321");
        Scn scn = YashanDbOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.COMMIT_SCN_KEY);
        assertThat(scn).isNotNull();
        assertThat(scn.longValue()).isEqualTo(54321);
    }

    @Test
    void shouldLoadSnapshotPendingTransactions() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(YashanDbOffsetContext.SNAPSHOT_PENDING_TRANSACTIONS_KEY, "tx1:100,tx2:200");
        Map<String, Scn> txns = YashanDbOffsetContext.loadSnapshotPendingTransactions(offset);
        assertThat(txns).hasSize(2);
        assertThat(txns.get("tx1").longValue()).isEqualTo(100);
        assertThat(txns.get("tx2").longValue()).isEqualTo(200);
    }

    @Test
    void shouldLoadEmptySnapshotPendingTransactions() {
        Map<String, Object> offset = new HashMap<>();
        Map<String, Scn> txns = YashanDbOffsetContext.loadSnapshotPendingTransactions(offset);
        assertThat(txns).isEmpty();
    }

    @Test
    void shouldLoadSnapshotPendingTransactionsWithEmptyEntries() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(YashanDbOffsetContext.SNAPSHOT_PENDING_TRANSACTIONS_KEY, "tx1:100,,tx2:200");
        Map<String, Scn> txns = YashanDbOffsetContext.loadSnapshotPendingTransactions(offset);
        assertThat(txns).hasSize(2);
    }

    @Test
    void shouldLoadSnapshotScn() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(YashanDbOffsetContext.SNAPSHOT_SCN_KEY, "777");
        Scn scn = YashanDbOffsetContext.loadSnapshotScn(offset);
        assertThat(scn.longValue()).isEqualTo(777);
    }

    @Test
    void shouldLoadYstreamStartScn() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(YashanDbOffsetContext.YSTREAM_START_SCN_KEY, "888");
        Scn scn = YashanDbOffsetContext.loadYstreamStartScn(offset);
        assertThat(scn.longValue()).isEqualTo(888);
    }

    @Test
    void shouldLoadYstreamStartScnNullWhenMissing() {
        Map<String, Object> offset = new HashMap<>();
        Scn scn = YashanDbOffsetContext.loadYstreamStartScn(offset);
        assertThat(scn).isNull();
    }

    @Test
    void shouldLoadRecoverPositionWithStringScn() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, "1000");
        offset.put(SourceInfo.INSTANCE_ID_KEY, "1");
        offset.put(SourceInfo.GROUP_LSN_KEY, 100L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 5);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 3);
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);
        assertThat(pos).isNotNull();
        assertThat(pos.getCommitScn().getScn()).isEqualTo(1000);
        assertThat(pos.getLogPosition().getGroupLsn()).isEqualTo(100L);
        assertThat(pos.getLogPosition().getGroupOffset()).isEqualTo(5);
        assertThat(pos.getLogPosition().getBatchRowId()).isEqualTo(3);
        assertThat(pos.getLogPosition().getInstanceId()).isEqualTo((byte) 1);
    }

    @Test
    void shouldLoadRecoverPositionWithLongScn() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, 2000L);
        offset.put(SourceInfo.INSTANCE_ID_KEY, "2");
        offset.put(SourceInfo.GROUP_LSN_KEY, 200L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 10);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 7);
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);
        assertThat(pos).isNotNull();
        assertThat(pos.getCommitScn().getScn()).isEqualTo(2000);
    }

    @Test
    void shouldLoadRecoverPositionWithLongGroupOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, 3000L);
        offset.put(SourceInfo.INSTANCE_ID_KEY, "3");
        offset.put(SourceInfo.GROUP_LSN_KEY, 300L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 15L);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 9L);
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);
        assertThat(pos).isNotNull();
        assertThat(pos.getLogPosition().getGroupOffset()).isEqualTo(15);
        assertThat(pos.getLogPosition().getBatchRowId()).isEqualTo(9);
    }

    @Test
    void shouldLoadRecoverPositionWithIntegerScn() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, 4000);
        offset.put(SourceInfo.INSTANCE_ID_KEY, "4");
        offset.put(SourceInfo.GROUP_LSN_KEY, 400L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 20);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 11);
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);
        assertThat(pos).isNotNull();
        assertThat(pos.getCommitScn().getScn()).isEqualTo(4000);
    }

    @Test
    void shouldLoadRecoverPositionWithBase64InstanceId() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, "5000");
        offset.put(SourceInfo.INSTANCE_ID_KEY, "AAAAAAA="); // Base64 encoded byte 0
        offset.put(SourceInfo.GROUP_LSN_KEY, 500L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 25);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 13);
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);
        assertThat(pos).isNotNull();
    }

    @Test
    void shouldReturnNullRecoverPositionWhenScnMissing() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.INSTANCE_ID_KEY, "1");
        offset.put(SourceInfo.GROUP_LSN_KEY, 100L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 5);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 3);
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);
        assertThat(pos).isNull();
    }

    @Test
    void shouldCheckIsDigitForNumericString() {
        // Using reflection to test private isDigit method indirectly via loadRecoverPosition
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, "1000");
        offset.put(SourceInfo.INSTANCE_ID_KEY, "5");
        offset.put(SourceInfo.GROUP_LSN_KEY, 100L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 5);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 3);
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);
        assertThat(pos).isNotNull();
        // If isDigit works correctly, "5" should be parsed as digit, not base64
        assertThat(pos.getLogPosition().getInstanceId()).isEqualTo((byte) 5);
    }

    @Test
    void shouldCreateBuilder() {
        YashanDbOffsetContext.Builder builder = YashanDbOffsetContext.create();
        assertThat(builder).isNotNull();
    }
}
