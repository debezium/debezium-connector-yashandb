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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.yashandb.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractBlockingSnapshotTest;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

public class BlockingSnapshotIT extends AbstractBlockingSnapshotTest<YashanDbConnector> {

    private static final String A = "A";
    private static final String B = "B";
    private static final String SIGNAL = "DEBEZIUM_SIGNAL";

    private YashanDbConnection connection;

    @BeforeAll
    static void beforeAll() throws Exception {
        TestHelper.dropTestUser();
        TestHelper.createTestUser();
    }

    @BeforeEach
    void before() throws Exception {
        connection = TestHelper.connectedConnection();
        TestHelper.dropTables(connection, A, B, SIGNAL);
        TestHelper.createYStreamServer();
        TestHelper.stopYStreamIfRunning();

        TestHelper.createTableIgnoreExists(connection,
                "CREATE TABLE " + A + " (pk INT NOT NULL, aa INT, PRIMARY KEY (pk))");
        TestHelper.createTableIgnoreExists(connection,
                "CREATE TABLE " + B + " (pk INT NOT NULL, aa INT, PRIMARY KEY (pk))");
        TestHelper.createTableIgnoreExists(connection,
                "CREATE TABLE " + SIGNAL + " (id VARCHAR(64), type VARCHAR(32), data VARCHAR(2048))");

        // Register data tables AND signal table with YStream so signal INSERTs are captured by CDC
        TestHelper.addYStreamTables(A + "," + B + "," + SIGNAL);

        setConsumeTimeout(30, TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void after() throws Exception {
        stopConnector();
        if (connection != null) {
            TestHelper.dropTables(connection, A, B, SIGNAL);
            connection.close();
        }
    }

    @Override
    protected void waitForConnectorToStart() {
        super.waitForConnectorToStart();
        try {
            waitForStreamingRunning(connector(), server());
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Class<YashanDbConnector> connectorClass() {
        return YashanDbConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected String topicName() {
        return TestHelper.topicName(A.toUpperCase(Locale.ROOT));
    }

    @Override
    protected List<String> topicNames() {
        return List.of(TestHelper.topicName(A.toUpperCase(Locale.ROOT)), TestHelper.topicName(B.toUpperCase(Locale.ROOT)));
    }

    @Override
    protected String tableName() {
        return TestHelper.qualifiedTableName(A);
    }

    @Override
    protected List<String> tableNames() {
        return List.of(TestHelper.qualifiedTableName(A), TestHelper.qualifiedTableName(B));
    }

    @Override
    protected String tableDataCollectionId() {
        return TestHelper.qualifiedTableName(A);
    }

    @Override
    protected String escapedTableDataCollectionId() {
        return "\\\"" + TestHelper.TEST_SCHEMA + "\\\".\\\"" + A + "\\\"";
    }

    @Override
    protected List<String> tableDataCollectionIds() {
        return List.of(TestHelper.qualifiedTableName(A), TestHelper.qualifiedTableName(B));
    }

    @Override
    protected String signalTableName() {
        return TestHelper.qualifiedTableName(SIGNAL);
    }

    @Override
    protected Configuration.Builder config() {
        return baseConfig(YashanDbConnectorConfig.SnapshotMode.INITIAL)
                .with(YashanDbConnectorConfig.SCHEMA_INCLUDE_LIST, TestHelper.TEST_SCHEMA)
                .with(YashanDbConnectorConfig.SNAPSHOT_MODE_TABLES, TestHelper.TEST_SCHEMA + "." + A)
                .with(YashanDbConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    private Configuration.Builder baseConfig(YashanDbConnectorConfig.SnapshotMode snapshotMode) {
        return TestHelper.defaultConfig()
                .with(YashanDbConnectorConfig.SNAPSHOT_MODE, snapshotMode)
                .with(YashanDbConnectorConfig.SNAPSHOT_LOCKING_MODE, YashanDbConnectorConfig.SnapshotLockingMode.NONE)
                .with(YashanDbConnectorConfig.YSTREAM_SERVER_NAME, TestHelper.ystreamServerName())
                .with(YashanDbConnectorConfig.SIGNAL_DATA_COLLECTION, this.signalTableName())
                .with(YashanDbConnectorConfig.YSTREAM_CLIENT_RESPONSE_TIMEOUT, 600)
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 100);
    }

    @Override
    protected Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        return config()
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedDdl);
    }

    @Override
    protected Configuration.Builder historizedMutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        return mutableConfig(signalTableOnly, storeOnlyCapturedDdl)
                .with(YashanDbConnectorConfig.INCLUDE_SCHEMA_CHANGES, true);
    }

    @Override
    protected String valueFieldName() {
        return "AA";
    }

    @Override
    protected String pkFieldName() {
        return "PK";
    }

    @Override
    protected String alterTableAddColumnStatement(String tableName) {
        return "ALTER TABLE " + tableName + " ADD col3 INTEGER DEFAULT 0";
    }

    @Override
    protected String connector() {
        return Module.name();
    }

    @Override
    protected String server() {
        return TestHelper.server();
    }

    @Override
    protected int expectedDdlsCount() {
        return 4;
    }

    @Override
    protected void assertDdl(List<String> schemaChangesDdls) {
        assertThat(schemaChangesDdls.get(schemaChangesDdls.size() - 1)).isEqualTo("CREATE TABLE \"DBZ\".\"B\"\n" +
                "(\"PK\" INTEGER NOT NULL ENABLE,\n" +
                "\"AA\" INTEGER,\n" +
                "PRIMARY KEY (\"PK\")\n" +
                "USING INDEX\n" +
                "PCTFREE 8 INITRANS 2 MAXTRANS 255\n" +
                "TABLESPACE \"USERS\" ENABLE\n" +
                ") PCTFREE 8 INITRANS 2 MAXTRANS 255\n" +
                "LOGGING\n" +
                "TABLESPACE \"USERS\"\n" +
                "SEGMENT CREATION DEFERRED\n" +
                "ORGANIZATION HEAP");
    }
}
