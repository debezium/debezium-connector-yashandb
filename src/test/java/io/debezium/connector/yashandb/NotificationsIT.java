/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yashandb;

import java.nio.file.Files;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import io.debezium.config.Configuration;
import io.debezium.connector.yashandb.util.TestHelper;
import io.debezium.pipeline.notification.AbstractNotificationsIT;

public class NotificationsIT extends AbstractNotificationsIT<YashanDbConnector> {

    private YashanDbConnection connection;

    @BeforeEach
    void before() throws Exception {
        connection = TestHelper.testConnection();
        TestHelper.createYStreamServer();
        TestHelper.dropAllTables();
        TestHelper.stopYStreamIfRunning();
        connection.execute("CREATE TABLE a (pk numeric(9,0) primary key, aa numeric(9,0))");
        TestHelper.addYStreamTables("a");

        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void after() {
        stopConnector();

        TestHelper.dropAllTables();
    }

    protected List<String> collections() {
        return List.of(TestHelper.qualifiedTableName("A"));
    }

    @Override
    protected Class<YashanDbConnector> connectorClass() {
        return YashanDbConnector.class;
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(YashanDbConnectorConfig.SNAPSHOT_MODE, YashanDbConnectorConfig.SnapshotMode.INITIAL);
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
    protected String snapshotStatusResult() {
        return "COMPLETED";
    }
}
