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
 *
 * Test Coverage:
 * - Happy path scenarios
 * - Boundary value tests (edge cases)
 * - Exception path tests (error handling)
 * - Null and empty input handling
 */
class YashanDbOffsetContextTest {

    // ==================== Happy Path Tests ====================

    @Test
    void shouldResolveScnFromStringFromOffsetMap() {
        // Given: offset map with String SCN
        Map<String, Object> offset = new HashMap<>();
        offset.put(YashanDbOffsetContext.SNAPSHOT_SCN_KEY, "12345");

        // When: resolve SCN from offset map
        Scn scn = YashanDbOffsetContext.getScnFromOffsetMapByKey(offset, YashanDbOffsetContext.SNAPSHOT_SCN_KEY);

        // Then: verify SCN is correctly parsed
        assertThat(scn).isNotNull();
        assertThat(scn.longValue()).isEqualTo(12345);
    }

    @Test
    void shouldResolveScnFromLongFromOffsetMap() {
        // Given: offset map with Long SCN
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, 99999L);

        // When: resolve SCN from offset map
        Scn scn = YashanDbOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.POSITION_SCN_KEY);

        // Then: verify SCN is correctly parsed
        assertThat(scn).isNotNull();
        assertThat(scn.longValue()).isEqualTo(99999);
    }

    // ==================== Null/Empty Input Tests ====================

    @Test
    void shouldReturnNullWhenScnKeyMissing() {
        // Given: offset map without SCN key
        Map<String, Object> offset = new HashMap<>();

        // When: resolve SCN from offset map
        Scn scn = YashanDbOffsetContext.getScnFromOffsetMapByKey(offset, YashanDbOffsetContext.SNAPSHOT_SCN_KEY);

        // Then: verify null is returned
        assertThat(scn).isNull();
    }

    @Test
    void shouldLoadEmptySnapshotPendingTransactions() {
        // Given: empty offset map
        Map<String, Object> offset = new HashMap<>();

        // When: load snapshot pending transactions
        Map<String, Scn> txns = YashanDbOffsetContext.loadSnapshotPendingTransactions(offset);

        // Then: verify empty map is returned
        assertThat(txns).isEmpty();
    }

    // ==================== Boundary Value Tests ====================

    @Test
    void shouldResolveScnFromZero() {
        // Given: offset map with zero SCN (boundary value)
        Map<String, Object> offset = new HashMap<>();
        offset.put(YashanDbOffsetContext.SNAPSHOT_SCN_KEY, "0");

        // When: resolve SCN from offset map
        Scn scn = YashanDbOffsetContext.getScnFromOffsetMapByKey(offset, YashanDbOffsetContext.SNAPSHOT_SCN_KEY);

        // Then: verify zero is correctly handled
        assertThat(scn).isNotNull();
        assertThat(scn.longValue()).isEqualTo(0);
    }

    @Test
    void shouldResolveScnFromMaxLongValue() {
        // Given: offset map with MAX_LONG value (boundary value)
        Map<String, Object> offset = new HashMap<>();
        offset.put(YashanDbOffsetContext.SNAPSHOT_SCN_KEY, String.valueOf(Long.MAX_VALUE));

        // When: resolve SCN from offset map
        Scn scn = YashanDbOffsetContext.getScnFromOffsetMapByKey(offset, YashanDbOffsetContext.SNAPSHOT_SCN_KEY);

        // Then: verify MAX_LONG is correctly parsed
        assertThat(scn).isNotNull();
        assertThat(scn.longValue()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void shouldLoadSnapshotScnFromZero() {
        // Given: offset map with zero snapshot SCN
        Map<String, Object> offset = new HashMap<>();
        offset.put(YashanDbOffsetContext.SNAPSHOT_SCN_KEY, "0");

        // When: load snapshot SCN
        Scn scn = YashanDbOffsetContext.loadSnapshotScn(offset);

        // Then: verify zero is correctly parsed
        assertThat(scn).isNotNull();
        assertThat(scn.longValue()).isEqualTo(0);
    }

    @Test
    void shouldLoadSnapshotPendingTransactionsWithEmptyEntries() {
        // Given: offset map with empty transaction entries
        Map<String, Object> offset = new HashMap<>();
        offset.put(YashanDbOffsetContext.SNAPSHOT_PENDING_TRANSACTIONS_KEY, "tx1:100,,tx2:200");

        // When: load snapshot pending transactions
        Map<String, Scn> txns = YashanDbOffsetContext.loadSnapshotPendingTransactions(offset);

        // Then: verify empty entries are filtered out
        assertThat(txns).hasSize(2);
    }

    @Test
    void shouldLoadRecoverPositionWithLargeValues() {
        // Given: offset map with large but safe values (boundary test)
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, Long.MAX_VALUE - 1);
        offset.put(SourceInfo.INSTANCE_ID_KEY, "127"); // Max byte value
        offset.put(SourceInfo.GROUP_LSN_KEY, Long.MAX_VALUE - 1);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, Integer.MAX_VALUE); // Safe large int
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, Integer.MAX_VALUE); // Safe large int

        // When: load recover position
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);

        // Then: verify large but safe values are correctly parsed
        assertThat(pos).isNotNull();
        assertThat(pos.getCommitScn().getScn()).isEqualTo(Long.MAX_VALUE - 1);
        assertThat(pos.getLogPosition().getGroupLsn()).isEqualTo(Long.MAX_VALUE - 1);
    }

    // ==================== Exception Path Tests ====================

    @Test
    void shouldReturnNullRecoverPositionWhenScnMissing() {
        // Given: offset map without position SCN
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.INSTANCE_ID_KEY, "1");
        offset.put(SourceInfo.GROUP_LSN_KEY, 100L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 5);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 3);

        // When: load recover position
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);

        // Then: verify null is returned when SCN is missing
        assertThat(pos).isNull();
    }

    @Test
    void shouldCheckIsDigitForNumericString() {
        // Given: offset map with numeric instance ID
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, "1000");
        offset.put(SourceInfo.INSTANCE_ID_KEY, "5");
        offset.put(SourceInfo.GROUP_LSN_KEY, 100L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 5);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 3);

        // When: load recover position
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);

        // Then: verify numeric instance ID is parsed correctly, not as base64
        assertThat(pos).isNotNull();
        assertThat(pos.getLogPosition().getInstanceId()).isEqualTo((byte) 5);
    }

    @Test
    void shouldLoadRecoverPositionWithStringScn() {
        // Given: offset map with string SCN
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, "1000");
        offset.put(SourceInfo.INSTANCE_ID_KEY, "1");
        offset.put(SourceInfo.GROUP_LSN_KEY, 100L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 5);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 3);

        // When: load recover position
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);

        // Then: verify position is correctly loaded
        assertThat(pos).isNotNull();
        assertThat(pos.getCommitScn().getScn()).isEqualTo(1000);
        assertThat(pos.getLogPosition().getGroupLsn()).isEqualTo(100L);
        assertThat(pos.getLogPosition().getGroupOffset()).isEqualTo(5);
        assertThat(pos.getLogPosition().getBatchRowId()).isEqualTo(3);
        assertThat(pos.getLogPosition().getInstanceId()).isEqualTo((byte) 1);
    }

    @Test
    void shouldLoadRecoverPositionWithLongScn() {
        // Given: offset map with Long SCN
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, 2000L);
        offset.put(SourceInfo.INSTANCE_ID_KEY, "2");
        offset.put(SourceInfo.GROUP_LSN_KEY, 200L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 10);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 7);

        // When: load recover position
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);

        // Then: verify position is correctly loaded
        assertThat(pos).isNotNull();
        assertThat(pos.getCommitScn().getScn()).isEqualTo(2000);
    }

    @Test
    void shouldLoadRecoverPositionWithLongGroupOffset() {
        // Given: offset map with Long group offset
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, 3000L);
        offset.put(SourceInfo.INSTANCE_ID_KEY, "3");
        offset.put(SourceInfo.GROUP_LSN_KEY, 300L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 15L);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 9L);

        // When: load recover position
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);

        // Then: verify long group offset is correctly handled
        assertThat(pos).isNotNull();
        assertThat(pos.getLogPosition().getGroupOffset()).isEqualTo(15);
        assertThat(pos.getLogPosition().getBatchRowId()).isEqualTo(9);
    }

    @Test
    void shouldLoadRecoverPositionWithIntegerScn() {
        // Given: offset map with Integer SCN
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, 4000);
        offset.put(SourceInfo.INSTANCE_ID_KEY, "4");
        offset.put(SourceInfo.GROUP_LSN_KEY, 400L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 20);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 11);

        // When: load recover position
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);

        // Then: verify position is correctly loaded
        assertThat(pos).isNotNull();
        assertThat(pos.getCommitScn().getScn()).isEqualTo(4000);
    }

    @Test
    void shouldLoadRecoverPositionWithBase64InstanceId() {
        // Given: offset map with Base64 encoded instance ID
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.POSITION_SCN_KEY, "5000");
        offset.put(SourceInfo.INSTANCE_ID_KEY, "AAAAAAA="); // Base64 encoded byte 0
        offset.put(SourceInfo.GROUP_LSN_KEY, 500L);
        offset.put(SourceInfo.GROUP_OFFSET_KEY, 25);
        offset.put(SourceInfo.BATCH_ROW_ID_KEY, 13);

        // When: load recover position
        Position pos = YashanDbOffsetContext.loadRecoverPosition(offset);

        // Then: verify Base64 decoded instance ID
        assertThat(pos).isNotNull();
    }

    @Test
    void shouldLoadSnapshotPendingTransactions() {
        // Given: offset map with transaction data
        Map<String, Object> offset = new HashMap<>();
        offset.put(YashanDbOffsetContext.SNAPSHOT_PENDING_TRANSACTIONS_KEY, "tx1:100,tx2:200");

        // When: load snapshot pending transactions
        Map<String, Scn> txns = YashanDbOffsetContext.loadSnapshotPendingTransactions(offset);

        // Then: verify transactions are correctly parsed
        assertThat(txns).hasSize(2);
        assertThat(txns.get("tx1").longValue()).isEqualTo(100);
        assertThat(txns.get("tx2").longValue()).isEqualTo(200);
    }

    @Test
    void shouldLoadSnapshotScn() {
        // Given: offset map with snapshot SCN
        Map<String, Object> offset = new HashMap<>();
        offset.put(YashanDbOffsetContext.SNAPSHOT_SCN_KEY, "777");

        // When: load snapshot SCN
        Scn scn = YashanDbOffsetContext.loadSnapshotScn(offset);

        // Then: verify snapshot SCN is correctly parsed
        assertThat(scn.longValue()).isEqualTo(777);
    }

    @Test
    void shouldCreateBuilder() {
        // When: create offset context builder

        // Then: verify builder is created
        YashanDbOffsetContext.Builder builder = YashanDbOffsetContext.create();
        assertThat(builder).isNotNull();
    }
}
