/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static io.debezium.connector.yashandb.util.TestHelper.addYStreamTables;

import java.nio.file.Files;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import io.debezium.config.Configuration;
import io.debezium.connector.yashandb.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractChunkedSnapshotTest;

/**
 * YashanDB-specific chunked table snapshot integration tests.
 */
public class YashanDbChunkedSnapshotIT extends AbstractChunkedSnapshotTest<YashanDbConnector> {

    private YashanDbConnection connection;

    @BeforeAll
    static void beforeAll() throws Exception {
        io.debezium.connector.yashandb.util.TestHelper.dropTestUser();
        io.debezium.connector.yashandb.util.TestHelper.createTestUser();

    }

    @BeforeEach
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();
        TestHelper.dropAllTables();
        TestHelper.createYStreamServer();
        TestHelper.stopYStreamIfRunning();
        setConsumeTimeout(20, TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        super.beforeEach();
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.afterEach();
    }

    @Override
    protected void populateSingleKeyTable(String tableName, int rowCount) throws SQLException {
        super.populateSingleKeyTable(tableName, rowCount);
        try {
            addYStreamTables(tableName);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void populateCompositeKeyTable(String tableName, int rowCount) throws SQLException {
        super.populateCompositeKeyTable(tableName, rowCount);
        try {
            addYStreamTables(tableName);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Class<YashanDbConnector> getConnectorClass() {
        return YashanDbConnector.class;
    }

    @Override
    protected JdbcConnection getConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfig() {
        return TestHelper.defaultConfig();
    }

    @Override
    protected void waitForSnapshotToBeCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted(connector(), server());
    }

    @Override
    protected void waitForStreamingRunning() throws InterruptedException {
        waitForStreamingRunning(connector(), server());
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
    protected String getSingleKeyCollectionName() {
        return TestHelper.TEST_SCHEMA + ".DBZ1220";
    }

    @Override
    protected String getCompositeKeyCollectionName() {
        return getSingleKeyCollectionName();
    }

    @Override
    protected String getMultipleSingleKeyCollectionNames() {
        return String.join(",", List.of("DBZ.DBZ1220A", "DBZ.DBZ1220B", "DBZ.DBZ1220C", "DBZ.DBZ1220D"));
    }

    @Override
    protected void createSingleKeyTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0) primary key, data varchar2(50))".formatted(tableName));
    }

    @Override
    protected void createCompositeKeyTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0), org_name varchar2(50), data varchar2(50), primary key(id, org_name))".formatted(tableName));
    }

    @Override
    protected void createKeylessTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0), data varchar2(50))".formatted(tableName));
    }

    @Override
    protected String getSingleKeyTableKeyColumnName() {
        return "ID";
    }

    @Override
    protected List<String> getCompositeKeyTableKeyColumnNames() {
        return List.of("ID", "ORG_NAME");
    }

    @Override
    protected String getTableTopicName(String tableName) {
        return server() + "." + "DBZ.%s".formatted(tableName.toUpperCase());
    }

    @Override
    protected String getFullyQualifiedTableName(String tableName) {
        return "DBZ.%s".formatted(tableName.toUpperCase());
    }

}
