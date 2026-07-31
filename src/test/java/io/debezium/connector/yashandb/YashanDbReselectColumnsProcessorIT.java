/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import io.debezium.config.Configuration;
import io.debezium.connector.yashandb.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.processors.AbstractReselectProcessorTest;
import io.debezium.processors.reselect.ReselectColumnsPostProcessor;
import io.debezium.util.Testing;

/**
 * YashanDB's integration tests for {@link ReselectColumnsPostProcessor}.
 */
public class YashanDbReselectColumnsProcessorIT extends AbstractReselectProcessorTest<YashanDbConnector> {

    private YashanDbConnection connection;

    @BeforeAll
    static void beforeAll() throws Exception {
        TestHelper.dropTestUser();
        TestHelper.createTestUser();
        TestHelper.createYStreamServer();
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        connection = TestHelper.connectedConnection();
        setConsumeTimeout(20, TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        super.beforeEach();
    }

    @AfterEach
    public void afterEach() throws Exception {
        super.afterEach();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    protected Class<YashanDbConnector> getConnectorClass() {
        return YashanDbConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder() {
        return TestHelper.defaultConfig()
                .with(YashanDbConnectorConfig.TABLE_INCLUDE_LIST, "DBZ\\.DBZ4321")
                .with(YashanDbConnectorConfig.CUSTOM_POST_PROCESSORS, "reselector")
                .with("post.processors.reselector.type", ReselectColumnsPostProcessor.class.getName());
    }

    @Override
    protected String topicName() {
        return TestHelper.topicName("DBZ4321");
    }

    @Override
    protected String tableName() {
        return TestHelper.qualifiedTableName("DBZ4321");
    }

    @Override
    protected String reselectColumnsList() {
        return "DBZ.DBZ4321:DATA";
    }

    @Override
    protected void createTable() throws Exception {
        TestHelper.dropTable(connection, "dbz4321");
        connection.execute("CREATE TABLE dbz4321 (id numeric(9,0) primary key, data varchar2(50), data2 numeric(9,0))");
        TestHelper.addYStreamTables("dbz4321");
    }

    @Override
    protected void dropTable() throws Exception {
        TestHelper.dropTable(connection, "dbz4321");
    }

    @Override
    protected String getInsertWithValue() {
        return "INSERT INTO dbz4321 (id,data,data2) values (1,'one',1)";
    }

    @Override
    protected String getInsertWithNullValue() {
        return "INSERT INTO dbz4321 (id,data,data2) values (1,null,1)";
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning(Module.name(), TestHelper.server());
    }

    @Override
    protected String fieldName(String fieldName) {
        return fieldName.toUpperCase();
    }
}
