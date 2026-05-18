/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.config.Field;

/**
 * Unit tests for {@link YashanDbConnectorConfig} configuration constants.
 */
class YashanDbConnectorConfigConstantsTest {

    @Test
    void shouldHaveDefaultPort() {
        assertThat(YashanDbConnectorConfig.DEFAULT_PORT).isEqualTo(1688);
    }

    @Test
    void shouldHaveDefaultQueryFetchSize() {
        assertThat(YashanDbConnectorConfig.DEFAULT_QUERY_FETCH_SIZE).isEqualTo(10_000);
    }

    @Test
    void shouldHaveExcludedSchemas() {
        List<String> excludedSchemas = YashanDbConnectorConfig.EXCLUDED_SCHEMAS;
        assertThat(excludedSchemas).isNotNull();
        assertThat(excludedSchemas).isNotEmpty();
        assertThat(excludedSchemas).contains("SYS");
        assertThat(excludedSchemas).contains("MDSYS");
        assertThat(excludedSchemas).contains("XA_SYS");
    }

    @Test
    void shouldHaveAllFields() {
        Field.Set allFields = YashanDbConnectorConfig.ALL_FIELDS;
        assertThat(allFields).isNotNull();
        assertThat(allFields).isNotEmpty();
    }

    @Test
    void shouldHavePortField() {
        assertThat(YashanDbConnectorConfig.PORT).isNotNull();
        assertThat(YashanDbConnectorConfig.PORT.name()).isEqualTo("database.port");
    }

    @Test
    void shouldHaveHostnameField() {
        assertThat(YashanDbConnectorConfig.HOSTNAME).isNotNull();
        assertThat(YashanDbConnectorConfig.HOSTNAME.name()).isEqualTo("database.hostname");
    }

    @Test
    void shouldHaveUserField() {
        assertThat(YashanDbConnectorConfig.USER).isNotNull();
    }

    @Test
    void shouldHavePasswordField() {
        assertThat(YashanDbConnectorConfig.PASSWORD).isNotNull();
    }

    @Test
    void shouldHaveDatabaseNameField() {
        assertThat(YashanDbConnectorConfig.DATABASE_NAME).isNotNull();
    }

    @Test
    void shouldHaveSnapshotModeField() {
        assertThat(YashanDbConnectorConfig.SNAPSHOT_MODE).isNotNull();
        assertThat(YashanDbConnectorConfig.SNAPSHOT_MODE.name()).isEqualTo("snapshot.mode");
    }

    @Test
    void shouldHaveUrlField() {
        assertThat(YashanDbConnectorConfig.URL).isNotNull();
        assertThat(YashanDbConnectorConfig.URL.name()).isEqualTo("database.url");
    }

    @Test
    void shouldHaveYstreamServerNameField() {
        assertThat(YashanDbConnectorConfig.YSTREAM_SERVER_NAME).isNotNull();
        assertThat(YashanDbConnectorConfig.YSTREAM_SERVER_NAME.name()).isEqualTo("database.ystream.server.name");
    }

    @Test
    void shouldHaveLobEnabledField() {
        assertThat(YashanDbConnectorConfig.LOB_ENABLED).isNotNull();
        assertThat(YashanDbConnectorConfig.LOB_ENABLED.name()).isEqualTo("lob.enabled");
    }

    @Test
    void shouldHaveIntervalHandlingModeField() {
        assertThat(YashanDbConnectorConfig.INTERVAL_HANDLING_MODE).isNotNull();
        assertThat(YashanDbConnectorConfig.INTERVAL_HANDLING_MODE.name()).isEqualTo("interval.handling.mode");
    }

    @Test
    void shouldHaveSnapshotLockingModeField() {
        assertThat(YashanDbConnectorConfig.SNAPSHOT_LOCKING_MODE).isNotNull();
        assertThat(YashanDbConnectorConfig.SNAPSHOT_LOCKING_MODE.name()).isEqualTo("snapshot.locking.mode");
    }

    @Test
    void shouldHaveYstreamQueueSizeField() {
        assertThat(YashanDbConnectorConfig.YSTREAM_QUEUE_SIZE).isNotNull();
        assertThat(YashanDbConnectorConfig.YSTREAM_QUEUE_SIZE.name()).isEqualTo("ystream.blocking.queue.size");
    }

    @Test
    void shouldHaveYstreamPollTimeoutField() {
        assertThat(YashanDbConnectorConfig.YSTREAM_POLL_TIMEOUT).isNotNull();
        assertThat(YashanDbConnectorConfig.YSTREAM_POLL_TIMEOUT.name()).isEqualTo("ystream.poll.timeout");
    }

    @Test
    void shouldHaveYstreamClientResponseTimeoutField() {
        assertThat(YashanDbConnectorConfig.YSTREAM_CLIENT_RESPONSE_TIMEOUT).isNotNull();
        assertThat(YashanDbConnectorConfig.YSTREAM_CLIENT_RESPONSE_TIMEOUT.name()).isEqualTo("ystream.client.response.timeout");
    }

    @Test
    void shouldHaveLogicShardEnabledField() {
        assertThat(YashanDbConnectorConfig.LOGIC_SHARD_ENABLED).isNotNull();
        assertThat(YashanDbConnectorConfig.LOGIC_SHARD_ENABLED.name()).isEqualTo("logic.shard.enabled");
    }

    @Test
    void shouldHaveTableReadThreadsField() {
        assertThat(YashanDbConnectorConfig.TABLE_READ_THREADS).isNotNull();
        assertThat(YashanDbConnectorConfig.TABLE_READ_THREADS.name()).isEqualTo("table.read.threads");
    }

    @Test
    void shouldHaveDdlParseFailRetryReadTableField() {
        assertThat(YashanDbConnectorConfig.DDL_PARSE_FAIL_RETRY_READ_TABLE).isNotNull();
        assertThat(YashanDbConnectorConfig.DDL_PARSE_FAIL_RETRY_READ_TABLE.name()).isEqualTo("ddl.parse.fail.retry.read.table");
    }

    @Test
    void shouldHaveLegacyDecimalHandlingStrategyField() {
        assertThat(YashanDbConnectorConfig.LEGACY_DECIMAL_HANDLING_STRATEGY).isNotNull();
        assertThat(YashanDbConnectorConfig.LEGACY_DECIMAL_HANDLING_STRATEGY.name()).isEqualTo("legacy.decimal.handling.strategy");
    }

    @Test
    void shouldHaveSnapshotDatabaseErrorsMaxRetriesField() {
        assertThat(YashanDbConnectorConfig.SNAPSHOT_DATABASE_ERRORS_MAX_RETRIES).isNotNull();
        assertThat(YashanDbConnectorConfig.SNAPSHOT_DATABASE_ERRORS_MAX_RETRIES.name()).isEqualTo("snapshot.database.errors.max.retries");
    }

    @Test
    void shouldHaveSourceInfoStructMakerField() {
        assertThat(YashanDbConnectorConfig.SOURCE_INFO_STRUCT_MAKER).isNotNull();
    }

    @Test
    void shouldHaveQueryFetchSizeField() {
        assertThat(YashanDbConnectorConfig.QUERY_FETCH_SIZE).isNotNull();
    }

    @Test
    void shouldHaveSnapshotEnhancementTokenField() {
        assertThat(YashanDbConnectorConfig.SNAPSHOT_ENHANCEMENT_TOKEN).isNotNull();
        assertThat(YashanDbConnectorConfig.SNAPSHOT_ENHANCEMENT_TOKEN.name()).isEqualTo("snapshot.enhance.predicate.scn");
    }
}
