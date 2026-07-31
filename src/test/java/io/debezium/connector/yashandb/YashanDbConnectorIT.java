/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotType;
import io.debezium.connector.yashandb.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Testing;

/**
 * Integration tests for the YashanDB connector.
 */
public class YashanDbConnectorIT extends AbstractAsyncEngineConnectorTest {

    private static final String CUSTOMERS = "DBZ_CUSTOMERS";
    private static final String PRODUCTS = "DBZ_PRODUCTS";

    private static YashanDbConnection connection;

    @BeforeAll
    static void beforeAll() throws Exception {
        TestHelper.createTestUser();
        TestHelper.createYStreamServer();
        connection = TestHelper.connectedConnection();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @BeforeEach
    void beforeEach() {
        setConsumeTimeout(30, TimeUnit.SECONDS);
        dropTable(CUSTOMERS);
        dropTable(PRODUCTS);
    }

    @AfterEach
    void afterEach() {
        stopConnector();
    }

    @Test
    void shouldTakeInitialOnlySnapshot() throws Exception {
        createCustomersTable();
        connection.execute("INSERT INTO " + CUSTOMERS + " (ID, NAME) VALUES (1, 'Alice')");
        connection.execute("INSERT INTO " + CUSTOMERS + " (ID, NAME) VALUES (2, 'Bob')");

        start(YashanDbConnector.class, snapshotOnlyConfig(CUSTOMERS));
        assertConnectorIsRunning();

        List<SourceRecord> records = consumeRecordsByTopic(2).recordsForTopic(TestHelper.topicName(CUSTOMERS));
        assertThat(records).hasSize(2);

        assertSnapshotRecord(records.get(0), 1, "Alice", false);
        assertSnapshotRecord(records.get(1), 2, "Bob", true);
    }

    @Test
    void shouldStopAfterInitialOnlySnapshot() throws Exception {
        createCustomersTable();
        connection.execute("INSERT INTO " + CUSTOMERS + " (ID, NAME) VALUES (1, 'Alice')");

        start(YashanDbConnector.class, snapshotOnlyConfig(CUSTOMERS));
        assertConnectorIsRunning();

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(TestHelper.topicName(CUSTOMERS));
        assertThat(records).hasSize(1);

        connection.execute("INSERT INTO " + CUSTOMERS + " (ID, NAME) VALUES (2, 'Bob')");
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        assertNoRecordsToConsume();
    }

    @Test
    void shouldApplyTableIncludeListDuringSnapshot() throws Exception {
        createCustomersTable();
        createProductsTable();
        connection.execute("INSERT INTO " + CUSTOMERS + " (ID, NAME) VALUES (1, 'Alice')");
        connection.execute("INSERT INTO " + PRODUCTS + " (ID, NAME) VALUES (1, 'Skipped')");

        start(YashanDbConnector.class, snapshotOnlyConfig(CUSTOMERS));
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TestHelper.topicName(CUSTOMERS))).hasSize(1);
        assertThat(records.recordsForTopic(TestHelper.topicName(PRODUCTS))).isNullOrEmpty();
    }

    private static Configuration snapshotOnlyConfig(String tableName) {
        return TestHelper.defaultConfig()
                .with(YashanDbConnectorConfig.SNAPSHOT_MODE, YashanDbConnectorConfig.SnapshotMode.INITIAL_ONLY)
                .with(YashanDbConnectorConfig.SNAPSHOT_LOCKING_MODE, YashanDbConnectorConfig.SnapshotLockingMode.NONE)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, TestHelper.TEST_SCHEMA + "\\." + tableName.toUpperCase(Locale.ROOT))
                .build();
    }

    private static void createCustomersTable() throws Exception {
        connection.execute("CREATE TABLE " + CUSTOMERS + " (ID INT NOT NULL, NAME VARCHAR(32), PRIMARY KEY (ID))");
    }

    private static void createProductsTable() throws Exception {
        connection.execute("CREATE TABLE " + PRODUCTS + " (ID INT NOT NULL, NAME VARCHAR(32), PRIMARY KEY (ID))");
    }

    private static void assertSnapshotRecord(SourceRecord record, int id, String name, boolean snapshotCompleted) {
        VerifyRecord.isValidRead(record, "ID", id);
        assertThat(record.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(SnapshotType.INITIAL.toString());
        assertThat(record.sourceOffset().get(YashanDbOffsetContext.SNAPSHOT_COMPLETED_KEY)).isEqualTo(snapshotCompleted);

        Struct value = (Struct) record.value();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(id);
        assertThat(after.get("NAME")).isEqualTo(name);

        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        assertThat(source.getString(SourceInfo.SCHEMA_NAME_KEY)).isEqualTo(TestHelper.TEST_SCHEMA);
        assertThat(source.getString(SourceInfo.TABLE_NAME_KEY)).isEqualTo(CUSTOMERS);
    }

    private static void dropTable(String tableName) {
        try {
            TestHelper.dropTable(connection, tableName);
        }
        catch (RuntimeException e) {
            Testing.print("Could not drop table " + tableName + ": " + e.getMessage());
        }
    }
}
