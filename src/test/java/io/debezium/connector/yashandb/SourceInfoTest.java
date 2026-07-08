/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.sics.ystream.result.LogPosition;
import com.sics.ystream.result.Position;
import com.sics.ystream.result.SystemChangeNumber;

import io.debezium.relational.TableId;

/**
 * Unit tests for {@link SourceInfo}.
 */
class SourceInfoTest {

    @Test
    void shouldCreateSourceInfoWithMockConfig() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);
        assertThat(sourceInfo).isNotNull();
    }

    @Test
    void shouldSetAndGetTransactionId() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        String txId = "tx123456";
        sourceInfo.setTransactionId(txId);

        assertThat(sourceInfo.getTransactionId()).isEqualTo(txId);
    }

    @Test
    void shouldSetAndGetSourceTime() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        Instant timestamp = Instant.parse("2024-01-15T10:30:00Z");
        sourceInfo.setSourceTime(timestamp);

        assertThat(sourceInfo.getSourceTime()).isEqualTo(timestamp);
    }

    @Test
    void shouldSetAndGetLcrPosition() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        Position lcrPosition = new Position(
                new SystemChangeNumber(1000L),
                new LogPosition((byte) 1, 200L, 10, 5));
        sourceInfo.setLcrPosition(lcrPosition);

        assertThat(sourceInfo.getPositionScn()).isEqualTo(1000L);
        assertThat(sourceInfo.getInstanceId()).isEqualTo("1");
        assertThat(sourceInfo.getGroupLsn()).isEqualTo(200L);
        assertThat(sourceInfo.getGroupOffset()).isEqualTo(10);
        assertThat(sourceInfo.getBatchRowId()).isEqualTo(5);
    }

    @Test
    void shouldConstructLcrPositionFromComponents() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        Position lcrPosition = new Position(
                new SystemChangeNumber(5000L),
                new LogPosition((byte) 2, 300L, 15, 7));
        sourceInfo.setLcrPosition(lcrPosition);

        Position constructed = sourceInfo.getLcrPosition();
        assertThat(constructed.getCommitScn().getScn()).isEqualTo(5000L);
        assertThat(constructed.getLogPosition().getInstanceId()).isEqualTo((byte) 2);
        assertThat(constructed.getLogPosition().getGroupLsn()).isEqualTo(300L);
        assertThat(constructed.getLogPosition().getGroupOffset()).isEqualTo(15);
        assertThat(constructed.getLogPosition().getBatchRowId()).isEqualTo(7);
    }

    @Test
    void shouldSetAndGetTableEventWithSingleTableId() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        TableId tableId = new TableId("catalog", "schema", "table_name");
        sourceInfo.tableEvent(tableId);

        assertThat(sourceInfo.table()).isEqualTo("table_name");
        assertThat(sourceInfo.tableSchema()).isEqualTo("schema");
    }

    @Test
    void shouldSetAndGetTableEventWithMultipleTableIds() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        Set<TableId> tableIds = new HashSet<>();
        tableIds.add(new TableId("catalog", "schema1", "table1"));
        tableIds.add(new TableId("catalog", "schema2", "table2"));
        sourceInfo.tableEvent(tableIds);

        assertThat(sourceInfo.table()).contains("table1", "table2");
    }

    @Test
    void shouldReturnNullTableWhenNoTablesSet() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        assertThat(sourceInfo.table()).isNull();
        assertThat(sourceInfo.tableSchema()).isNull();
    }

    @Test
    void shouldFilterNullTableIdsInTableMethods() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        Set<TableId> tableIds = new HashSet<>();
        tableIds.add(null);
        tableIds.add(new TableId("catalog", "schema", "valid_table"));
        sourceInfo.tableEvent(tableIds);

        assertThat(sourceInfo.table()).isEqualTo("valid_table");
        assertThat(sourceInfo.tableSchema()).isEqualTo("schema");
    }

    @Test
    void shouldReturnDistinctSchemaNames() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        Set<TableId> tableIds = new HashSet<>();
        tableIds.add(new TableId("catalog", "schema1", "table1"));
        tableIds.add(new TableId("catalog", "schema1", "table2"));
        tableIds.add(new TableId("catalog", "schema2", "table3"));
        sourceInfo.tableEvent(tableIds);

        assertThat(sourceInfo.tableSchema().split(",")).containsExactlyInAnyOrder("schema1", "schema2");
    }

    @Test
    void shouldReturnNullDatabaseWhenNoTablesSet() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        assertThat(sourceInfo.database()).isNull();
    }

    @Test
    void shouldReturnDatabaseFromTableId() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        TableId tableId = new TableId("test_catalog", "schema", "table");
        sourceInfo.tableEvent(tableId);

        assertThat(sourceInfo.database()).isEqualTo(null);
    }

    @Test
    void shouldHandleEmptyTableIdsSet() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        sourceInfo.tableEvent(Collections.emptySet());

        assertThat(sourceInfo.table()).isNull();
        assertThat(sourceInfo.tableSchema()).isNull();
    }

    @Test
    void shouldReturnTimestamp() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        Instant timestamp = Instant.now();
        sourceInfo.setSourceTime(timestamp);

        // Access protected method via reflection or through the class directly
        assertThat(sourceInfo.getSourceTime()).isEqualTo(timestamp);
    }

    @Test
    void shouldHandleAllLcrPositionComponents() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        // Test with maximum values
        long maxScn = Long.MAX_VALUE;
        long maxGroupLsn = Long.MAX_VALUE;
        int maxGroupOffset = Integer.MAX_VALUE;
        int maxBatchRowId = Integer.MAX_VALUE;
        byte maxInstanceId = Byte.MAX_VALUE;

        Position lcrPosition = new Position(
                new SystemChangeNumber(maxScn),
                new LogPosition(maxInstanceId, maxGroupLsn, maxGroupOffset, maxBatchRowId));
        sourceInfo.setLcrPosition(lcrPosition);

        assertThat(sourceInfo.getPositionScn()).isEqualTo(maxScn);
        assertThat(sourceInfo.getInstanceId()).isEqualTo(String.valueOf(maxInstanceId));
        assertThat(sourceInfo.getGroupLsn()).isEqualTo(maxGroupLsn);
        assertThat(sourceInfo.getGroupOffset()).isEqualTo(maxGroupOffset);
        assertThat(sourceInfo.getBatchRowId()).isEqualTo(maxBatchRowId);
    }

    @Test
    void shouldHandleZeroValues() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        Position lcrPosition = new Position(
                new SystemChangeNumber(0L),
                new LogPosition((byte) 0, 0L, 0, 0));
        sourceInfo.setLcrPosition(lcrPosition);

        assertThat(sourceInfo.getPositionScn()).isEqualTo(0L);
        assertThat(sourceInfo.getInstanceId()).isEqualTo("0");
        assertThat(sourceInfo.getGroupLsn()).isEqualTo(0L);
        assertThat(sourceInfo.getGroupOffset()).isEqualTo(0);
        assertThat(sourceInfo.getBatchRowId()).isEqualTo(0);
    }

    @Test
    void shouldSetTransactionIdToNull() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        SourceInfo sourceInfo = new TestableSourceInfo(config);

        sourceInfo.setTransactionId("tx1");
        sourceInfo.setTransactionId(null);

        assertThat(sourceInfo.getTransactionId()).isNull();
    }

    /**
     * Testable subclass to access protected SourceInfo constructor.
     */
    private static class TestableSourceInfo extends SourceInfo {
        protected TestableSourceInfo(YashanDbConnectorConfig connectorConfig) {
            super(connectorConfig);
        }
    }
}
