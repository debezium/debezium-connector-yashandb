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
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Guards DBZ-1850: a table containing a UDT column (such as {@code ST_GEOMETRY}, as in
 * {@code SZ_NCIS_ORDER.ORD_EXT_ZGEOMETRY}) that enters the YStream capture scope makes the streaming task fail,
 * because the YStream client rejects the UDT metadata with {@code "UDT is not supported yet"}. Passing
 * {@code ystream.fault.tolerance=true} through to the client (see
 * {@link io.debezium.connector.yashandb.ystream.YStreamStreamingChangeEventSource}) makes the
 * client tolerate the UDT metadata so that streaming continues for the regular tables.
 *
 * <p>The UDT table is registered with the YStream server, but is intentionally excluded from the connector's
 * {@code table.include.list}: the failure that this test guards is raised while the YStream client parses the
 * metadata of the newly captured table, before any connector-side filtering takes place.</p>
 */
public class YStreamFaultToleranceIT extends AbstractAsyncEngineConnectorTest {

    private static final String PLAIN_TABLE = "DBZ_YSTREAM_FAULT_TOLERANCE";
    // Mirrors SZ_NCIS_ORDER.ORD_EXT_ZGEOMETRY from the DBZ-1850 incident: it contains an ST_GEOMETRY (UDT) column.
    private static final String UDT_TABLE = "DBZ_ORD_EXT_ZGEOMETRY";
    private static final int CLIENT_RESPONSE_TIMEOUT_SECONDS = 2;
    // Small retry budget so the failure test completes quickly instead of exhausting the default 30 attempts.
    private static final int RETRY_MAX_ATTEMPTS = 3;

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
        TestHelper.dropTables(connection, PLAIN_TABLE, UDT_TABLE);
        connection.execute("CREATE TABLE " + PLAIN_TABLE
                + " (ID INT NOT NULL, NAME VARCHAR(32), PRIMARY KEY (ID))");
        connection.execute("CREATE TABLE " + UDT_TABLE
                + " (ID INT NOT NULL, SHAPE ST_GEOMETRY, PRIMARY KEY (ID))");
        TestHelper.addYStreamTables(PLAIN_TABLE + "," + UDT_TABLE);
    }

    @AfterEach
    void afterEach() throws Exception {
        stopConnector();
        TestHelper.dropTables(connection, PLAIN_TABLE, UDT_TABLE);
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldFailStreamingWhenUdtTableIsCapturedWithoutFaultTolerance() throws Exception {
        CountDownLatch completion = new CountDownLatch(1);
        AtomicBoolean completedSuccessfully = new AtomicBoolean(true);
        AtomicReference<Throwable> completionFailure = new AtomicReference<>();

        start(YashanDbConnector.class, streamingConfig(false), (success, message, error) -> {
            completedSuccessfully.set(success);
            completionFailure.set(error);
            completion.countDown();
        });
        assertConnectorIsRunning();

        // A change on the newly captured UDT table forces the client to parse its metadata.
        insertUdtRow(1);

        await().atMost(Duration.ofSeconds(120))
                .until(() -> completion.getCount() == 0);
        assertThat(completedSuccessfully).isFalse();
        assertThat(completionFailure.get())
                .isNotNull()
                .hasStackTraceContaining("UDT is not supported yet");
        assertConnectorNotRunning();
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldKeepStreamingWhenFaultToleranceIsEnabled() throws Exception {
        start(YashanDbConnector.class, streamingConfig(true));
        assertConnectorIsRunning();
        waitForStreamingRunning(Module.name(), TestHelper.server());

        // Changes on the UDT table must be tolerated without terminating the streaming task.
        insertUdtRow(1);

        insert(1, "before geometry");
        assertInsert(consumeTableRecord(), 1, "before geometry");
        insert(2, "after geometry");
        assertInsert(consumeTableRecord(), 2, "after geometry");
        assertConnectorIsRunning();
    }

    private static Configuration streamingConfig(boolean faultTolerance) {
        return TestHelper.defaultConfig()
                .with(CommonConnectorConfig.ERRORS_MAX_RETRIES, 0)
                .with(YashanDbConnectorConfig.SNAPSHOT_MODE, YashanDbConnectorConfig.SnapshotMode.NO_DATA)
                .with(YashanDbConnectorConfig.SNAPSHOT_LOCKING_MODE, YashanDbConnectorConfig.SnapshotLockingMode.NONE)
                .with(YashanDbConnectorConfig.YSTREAM_SERVER_NAME, TestHelper.ystreamServerName())
                .with(YashanDbConnectorConfig.YSTREAM_POLL_TIMEOUT, 1)
                .with(YashanDbConnectorConfig.YSTREAM_CLIENT_RESPONSE_TIMEOUT, CLIENT_RESPONSE_TIMEOUT_SECONDS)
                .with(YashanDbConnectorConfig.YSTREAM_RETRY_MAX_ATTEMPTS, RETRY_MAX_ATTEMPTS)
                .with(YashanDbConnectorConfig.YSTREAM_RETRY_BACKOFF_MS, 100)
                .with(YashanDbConnectorConfig.YSTREAM_RETRY_WINDOW_MS, 30_000)
                .with(YashanDbConnectorConfig.YSTREAM_FAULT_TOLERANCE, faultTolerance)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
                        TestHelper.TEST_SCHEMA + "\\." + PLAIN_TABLE)
                .build();
    }

    private static void insert(int id, String name) throws Exception {
        connection.execute("INSERT INTO " + PLAIN_TABLE + " (ID, NAME) VALUES (" + id + ", '" + name + "')");
    }

    private static void insertUdtRow(int id) throws Exception {
        connection.execute("INSERT INTO " + UDT_TABLE + " (ID) VALUES (" + id + ")");
    }

    private SourceRecord consumeTableRecord() throws InterruptedException {
        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(TestHelper.topicName(PLAIN_TABLE));
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
