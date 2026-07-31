/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static io.debezium.connector.yashandb.util.TestHelper.server;
import static io.debezium.connector.yashandb.util.TestHelper.ystreamServerName;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.yashandb.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Integration tests for YashanDB data type handling during streaming.
 */
public class YashanDbDataTypesIT extends AbstractAsyncEngineConnectorTest {

    private static final String DATATYPES = "DBZ_DATATYPES";

    private static YashanDbConnection connection;

    @BeforeAll
    static void beforeAll() throws Exception {
        TestHelper.createTestUser();
        TestHelper.createYStreamServer();
        connection = TestHelper.connectedConnection();
    }

    @BeforeEach
    void beforeEach() {
        setConsumeTimeout(30, TimeUnit.SECONDS);
        TestHelper.dropTable(connection, DATATYPES);
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void shouldStreamDifferentDataTypes() throws Exception {
        createDataTypesTable();
        TestHelper.addYStreamTables(DATATYPES);

        start(YashanDbConnector.class, streamingConfig(DATATYPES));
        assertConnectorIsRunning();
        waitForStreamingRunning(Module.name(), server());

        connection.execute("INSERT INTO " + DATATYPES + " ("
                + "ID, TINY_COL, SMALL_COL, INT_COL, BIG_COL, NUMBER_COL, DECIMAL_COL, FLOAT_COL, DOUBLE_COL, "
                + "CHAR_COL, VARCHAR_COL, NCHAR_COL, NVARCHAR_COL, RAW_COL, DATE_COL, TS_COL"
                + ") VALUES ("
                + "1, 7, 32000, 123456789, 1234567890123, 12345, 1234.56, 3.25, 6.5, "
                + "'A', 'varchar value', 'N', 'nvarchar value', HEXTORAW('0A0B0C'), "
                + "TO_DATE('2024-01-15 10:30:45', 'YYYY-MM-DD HH24:MI:SS'), "
                + "TO_TIMESTAMP('2024-01-15 10:30:45.123456')"
                + ")");

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(TestHelper.topicName(DATATYPES));
        assertThat(records).hasSize(1);

        SourceRecord record = records.get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct value = (Struct) record.value();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(((Number) after.get("TINY_COL")).intValue()).isEqualTo(7);
        assertThat(after.get("SMALL_COL")).isEqualTo((short) 32000);
        assertThat(after.get("INT_COL")).isEqualTo(123456789);
        assertThat(after.get("BIG_COL")).isEqualTo(1234567890123L);
        assertThat(((Number) after.get("NUMBER_COL")).longValue()).isEqualTo(12345L);
        assertThat(after.get("DECIMAL_COL")).isEqualTo(new BigDecimal("1234.56"));
        assertThat(after.get("FLOAT_COL")).isEqualTo(3.25f);
        assertThat(after.get("DOUBLE_COL")).isEqualTo(6.5D);
        assertThat(after.get("CHAR_COL")).isEqualTo("A");
        assertThat(after.get("VARCHAR_COL")).isEqualTo("varchar value");
        assertThat(after.get("NCHAR_COL")).isEqualTo("N");
        assertThat(after.get("NVARCHAR_COL")).isEqualTo("nvarchar value");
        assertThat(binaryValue(after.get("RAW_COL"))).containsExactly((byte) 0x0A, (byte) 0x0B, (byte) 0x0C);
        assertThat(after.get("DATE_COL")).isNotNull();
        assertThat(after.get("TS_COL")).isNotNull();

        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        assertThat(source.getString(SourceInfo.SCHEMA_NAME_KEY)).isEqualTo(TestHelper.TEST_SCHEMA);
        assertThat(source.getString(SourceInfo.TABLE_NAME_KEY)).isEqualTo(DATATYPES);
    }

    private static Configuration streamingConfig(String tableName) {
        return TestHelper.defaultConfig()
                .with(YashanDbConnectorConfig.SNAPSHOT_MODE, YashanDbConnectorConfig.SnapshotMode.NO_DATA)
                .with(YashanDbConnectorConfig.SNAPSHOT_LOCKING_MODE, YashanDbConnectorConfig.SnapshotLockingMode.NONE)
                .with(YashanDbConnectorConfig.YSTREAM_SERVER_NAME, ystreamServerName())
                .with(YashanDbConnectorConfig.LEGACY_DECIMAL_HANDLING_STRATEGY, true)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
                        TestHelper.TEST_SCHEMA + "\\." + tableName.toUpperCase(Locale.ROOT))
                .build();
    }

    private static void createDataTypesTable() throws Exception {
        connection.execute("CREATE TABLE " + DATATYPES + " ("
                + "ID INT NOT NULL, "
                + "TINY_COL TINYINT, "
                + "SMALL_COL SMALLINT, "
                + "INT_COL INT, "
                + "BIG_COL BIGINT, "
                + "NUMBER_COL NUMBER(10,0), "
                + "DECIMAL_COL NUMBER(10,2), "
                + "FLOAT_COL FLOAT, "
                + "DOUBLE_COL DOUBLE, "
                + "CHAR_COL CHAR(1), "
                + "VARCHAR_COL VARCHAR(32), "
                + "NCHAR_COL NCHAR(1), "
                + "NVARCHAR_COL NVARCHAR(32), "
                + "RAW_COL RAW(16), "
                + "DATE_COL DATE, "
                + "TS_COL TIMESTAMP(6), "
                + "PRIMARY KEY (ID))");
    }

    private static byte[] binaryValue(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer duplicate = buffer.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return bytes;
        }
        return new byte[0];
    }
}
