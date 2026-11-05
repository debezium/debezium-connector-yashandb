/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotType;
import io.debezium.connector.yashandb.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Integration coverage for an instance installed with YashanDB MySQL syntax mode.
 *
 * <p>The test is enabled by the {@code mysql-integration-tests} Maven profile.
 * Keeping it disabled for the default profile prevents a native-mode container
 * from being mistaken for a MySQL-mode database.</p>
 */
@EnabledIfSystemProperty(named = "mysql.integration.enabled", matches = "true")
public class YashanDbMySqlModeIT extends AbstractAsyncEngineConnectorTest {

    private static final String CUSTOMERS = "dbz_mysql_customers";
    private static YashanDbConnection connection;

    @BeforeAll
    static void beforeAll() throws Exception {
        TestHelper.createMySqlTestUser();
        TestHelper.createYStreamServer();
        connection = TestHelper.createMySqlMode();
        connection.connect();
    }

    @AfterAll
    static void afterAll() throws Exception {
        TestHelper.stopYStreamIfRunning();
        if (connection != null) {
            connection.close();
        }
    }

    @BeforeEach
    void beforeEach() {
        setConsumeTimeout(30, TimeUnit.SECONDS);
        TestHelper.dropTable(connection, CUSTOMERS);
    }

    @AfterEach
    void afterEach() {
        stopConnector();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldSnapshotTableUsingMySqlDatabaseAndTableIdentifiers() throws Exception {
        connection.execute("CREATE TABLE `" + CUSTOMERS + "` (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY (id))");
        connection.execute("INSERT INTO `" + CUSTOMERS + "` (id, name) VALUES (1, 'Alice')");
        connection.execute("INSERT INTO `" + CUSTOMERS + "` (id, name) VALUES (2, 'Bob')");
        TestHelper.recreateYStreamServerWithTables(CUSTOMERS);

        start(YashanDbConnector.class, mysqlSnapshotOnlyConfig());
        assertConnectorIsRunning();

        List<SourceRecord> records = consumeRecordsByTopic(2)
                .recordsForTopic(TestHelper.server() + "." + TestHelper.MYSQL_TEST_DATABASE + "." + CUSTOMERS);
        assertThat(records).hasSize(2);
        assertThat(records).extracting(record -> ((Struct) record.key()).getInt32("id"))
                .containsExactlyInAnyOrder(1, 2);
        for (int i = 0; i < records.size(); i++) {
            SourceRecord record = records.get(i);
            int id = ((Struct) record.key()).getInt32("id");
            assertSnapshotRecord(record, id, id == 1 ? "Alice" : "Bob", i == records.size() - 1);
        }
    }

    private static Configuration mysqlSnapshotOnlyConfig() {
        return TestHelper.defaultConfig()
                .with(YashanDbConnectorConfig.DATABASE_NAME, TestHelper.MYSQL_TEST_DATABASE)
                .with(YashanDbConnectorConfig.SNAPSHOT_MODE, YashanDbConnectorConfig.SnapshotMode.INITIAL_ONLY)
                .with(YashanDbConnectorConfig.SNAPSHOT_LOCKING_MODE, YashanDbConnectorConfig.SnapshotLockingMode.NONE)
                .with(YashanDbConnectorConfig.YSTREAM_ADDITIONAL_PROPERTIES_PREFIX + "mysql.mode", "true")
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, TestHelper.MYSQL_TEST_DATABASE + "\\." + CUSTOMERS)
                .build();
    }

    private static void assertSnapshotRecord(SourceRecord record, int id, String name, boolean snapshotCompleted) {
        VerifyRecord.isValidRead(record, "id", id);
        assertThat(record.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(SnapshotType.INITIAL.toString());
        assertThat(record.sourceOffset().get(YashanDbOffsetContext.SNAPSHOT_COMPLETED_KEY)).isEqualTo(snapshotCompleted);
        Struct value = (Struct) record.value();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("id")).isEqualTo(id);
        assertThat(after.get("name")).isEqualTo(name);

        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        assertThat(source.getString(SourceInfo.SCHEMA_NAME_KEY)).isEqualTo(TestHelper.MYSQL_TEST_DATABASE);
        assertThat(source.getString(SourceInfo.TABLE_NAME_KEY)).isEqualTo(CUSTOMERS);
    }
}
