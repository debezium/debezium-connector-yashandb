/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.yashandb.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

public class YStreamRetryIT extends AbstractAsyncEngineConnectorTest {

    private static final String TABLE_NAME = "DBZ_YSTREAM_RETRY";
    private static final int CLIENT_RESPONSE_TIMEOUT_SECONDS = 2;

    private static YashanDbConnection connection;

    @BeforeAll
    static void beforeAll() throws Exception {
        TestHelper.createTestUser();
        TestHelper.createYStreamServer();
        connection = TestHelper.connectedConnection();
    }

    @BeforeEach
    void beforeEach() throws Exception {
        setConsumeTimeout(45, TimeUnit.SECONDS);
        TestHelper.dropTable(connection, TABLE_NAME);
        connection.execute("CREATE TABLE " + TABLE_NAME
                + " (ID INT NOT NULL, NAME VARCHAR(32), PRIMARY KEY (ID))");
        TestHelper.addYStreamTables(TABLE_NAME);
    }

    @AfterEach
    void afterEach() throws Exception {
        stopConnector();
        try {
            TestHelper.startYStreamServer();
        }
        finally {
            TestHelper.dropTable(connection, TABLE_NAME);
        }
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void shouldResumeStreamingAfterYStreamServerRestart() throws Exception {
        start(YashanDbConnector.class, streamingConfig());
        assertConnectorIsRunning();
        waitForStreamingRunning(Module.name(), TestHelper.server());

        insert(1, "before outage");
        assertInsert(consumeTableRecord(), 1, "before outage");

        TestHelper.stopYStreamServer();
        Metronome retryMetronome = Metronome.sleeper(Duration.ofSeconds(CLIENT_RESPONSE_TIMEOUT_SECONDS + 1), Clock.system());
        retryMetronome.pause();
        assertConnectorIsRunning();

        TestHelper.startYStreamServer();
        insert(2, "after recovery");
        assertInsert(consumeTableRecord(), 2, "after recovery");
        assertConnectorIsRunning();
    }

    @Test
    void shouldFailAfterExhaustingYStreamRetries() throws Exception {
        CountDownLatch completion = new CountDownLatch(1);
        AtomicBoolean completedSuccessfully = new AtomicBoolean(true);
        AtomicReference<Throwable> completionFailure = new AtomicReference<>();

        start(YashanDbConnector.class, streamingConfig(), (success, message, error) -> {
            completedSuccessfully.set(success);
            completionFailure.set(error);
            completion.countDown();
        });
        assertConnectorIsRunning();
        waitForStreamingRunning(Module.name(), TestHelper.server());

        insert(1, "before outage");
        assertInsert(consumeTableRecord(), 1, "before outage");

        TestHelper.stopYStreamServer();

        await().atMost(Duration.ofSeconds(180))
                .until(() -> completion.getCount() == 0);
        assertThat(completedSuccessfully).isFalse();
        assertThat(completionFailure.get())
                .isNotNull()
                .hasStackTraceContaining("YStream read failed: 30 attempts within 180s window");
        assertConnectorNotRunning();
    }

    private static Configuration streamingConfig() {
        return TestHelper.defaultConfig()
                .with(CommonConnectorConfig.ERRORS_MAX_RETRIES, 0)
                .with(YashanDbConnectorConfig.SNAPSHOT_MODE, YashanDbConnectorConfig.SnapshotMode.NO_DATA)
                .with(YashanDbConnectorConfig.SNAPSHOT_LOCKING_MODE, YashanDbConnectorConfig.SnapshotLockingMode.NONE)
                .with(YashanDbConnectorConfig.YSTREAM_SERVER_NAME, TestHelper.ystreamServerName())
                .with(YashanDbConnectorConfig.YSTREAM_POLL_TIMEOUT, 1)
                .with(YashanDbConnectorConfig.YSTREAM_CLIENT_RESPONSE_TIMEOUT, CLIENT_RESPONSE_TIMEOUT_SECONDS)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
                        TestHelper.TEST_SCHEMA + "\\." + TABLE_NAME.toUpperCase(Locale.ROOT))
                .build();
    }

    private static void insert(int id, String name) throws Exception {
        connection.execute("INSERT INTO " + TABLE_NAME + " (ID, NAME) VALUES (" + id + ", '" + name + "')");
    }

    private SourceRecord consumeTableRecord() throws InterruptedException {
        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(TestHelper.topicName(TABLE_NAME));
        assertThat(records).hasSize(1);
        return records.get(0);
    }

    private static void assertInsert(SourceRecord record, int id, String name) {
        VerifyRecord.isValidInsert(record, "ID", id);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(id);
        assertThat(after.get("NAME")).isEqualTo(name);
    }
}
