/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.debezium.config.Field;
import io.debezium.connector.yashandb.YashanDbConnectorConfig.IntervalHandlingMode;
import io.debezium.connector.yashandb.YashanDbConnectorConfig.LogMiningQueryFilterMode;
import io.debezium.connector.yashandb.YashanDbConnectorConfig.LogMiningStrategy;
import io.debezium.connector.yashandb.YashanDbConnectorConfig.SnapshotLockingMode;
import io.debezium.connector.yashandb.YashanDbConnectorConfig.SnapshotMode;
import io.debezium.connector.yashandb.YashanDbConnectorConfig.TransactionSnapshotBoundaryMode;

/**
 * Unit tests for {@link YashanDbConnectorConfig} and its nested enums.
 */
class YashanDbConnectorConfigTest {

    // -----------------------------------------------------------------------
    // IntervalHandlingMode
    // -----------------------------------------------------------------------
    @Nested
    class IntervalHandlingModeTests {

        @Test
        void shouldHaveAllValues() {
            IntervalHandlingMode[] values = IntervalHandlingMode.values();
            assertThat(values).hasSize(2);
            assertThat(values).containsExactly(IntervalHandlingMode.NUMERIC, IntervalHandlingMode.STRING);
        }

        @Test
        void shouldReturnCorrectValueForNumeric() {
            assertThat(IntervalHandlingMode.NUMERIC.getValue()).isEqualTo("numeric");
        }

        @Test
        void shouldReturnCorrectValueForString() {
            assertThat(IntervalHandlingMode.STRING.getValue()).isEqualTo("string");
        }

        @Test
        void shouldParseValidValue() {
            assertThat(IntervalHandlingMode.parse("numeric")).isEqualTo(IntervalHandlingMode.NUMERIC);
            assertThat(IntervalHandlingMode.parse("string")).isEqualTo(IntervalHandlingMode.STRING);
        }

        @Test
        void shouldParseCaseInsensitive() {
            assertThat(IntervalHandlingMode.parse("NUMERIC")).isEqualTo(IntervalHandlingMode.NUMERIC);
            assertThat(IntervalHandlingMode.parse("Numeric")).isEqualTo(IntervalHandlingMode.NUMERIC);
            assertThat(IntervalHandlingMode.parse("STRING")).isEqualTo(IntervalHandlingMode.STRING);
            assertThat(IntervalHandlingMode.parse("String")).isEqualTo(IntervalHandlingMode.STRING);
        }

        @Test
        void shouldParseWithWhitespace() {
            assertThat(IntervalHandlingMode.parse("  numeric  ")).isEqualTo(IntervalHandlingMode.NUMERIC);
            assertThat(IntervalHandlingMode.parse("\tstring\n")).isEqualTo(IntervalHandlingMode.STRING);
        }

        @Test
        void shouldReturnNullForUnknownValue() {
            assertThat(IntervalHandlingMode.parse("unknown")).isNull();
            assertThat(IntervalHandlingMode.parse("foo")).isNull();
        }

        @Test
        void shouldReturnNullForNullInput() {
            assertThat(IntervalHandlingMode.parse(null)).isNull();
        }
    }

    // -----------------------------------------------------------------------
    // SnapshotMode
    // -----------------------------------------------------------------------
    @Nested
    class SnapshotModeTests {

        @Test
        void shouldHaveAllValues() {
            SnapshotMode[] values = SnapshotMode.values();
            assertThat(values).hasSize(5);
            assertThat(values).containsExactly(
                    SnapshotMode.ALWAYS,
                    SnapshotMode.INITIAL,
                    SnapshotMode.INITIAL_ONLY,
                    SnapshotMode.SCHEMA_ONLY,
                    SnapshotMode.SCHEMA_ONLY_RECOVERY);
        }

        @Test
        void shouldReturnCorrectValues() {
            assertThat(SnapshotMode.ALWAYS.getValue()).isEqualTo("always");
            assertThat(SnapshotMode.INITIAL.getValue()).isEqualTo("initial");
            assertThat(SnapshotMode.INITIAL_ONLY.getValue()).isEqualTo("initial_only");
            assertThat(SnapshotMode.SCHEMA_ONLY.getValue()).isEqualTo("schema_only");
            assertThat(SnapshotMode.SCHEMA_ONLY_RECOVERY.getValue()).isEqualTo("schema_only_recovery");
        }

        @Test
        void shouldParseValidValue() {
            assertThat(SnapshotMode.parse("always")).isEqualTo(SnapshotMode.ALWAYS);
            assertThat(SnapshotMode.parse("initial")).isEqualTo(SnapshotMode.INITIAL);
            assertThat(SnapshotMode.parse("initial_only")).isEqualTo(SnapshotMode.INITIAL_ONLY);
            assertThat(SnapshotMode.parse("schema_only")).isEqualTo(SnapshotMode.SCHEMA_ONLY);
            assertThat(SnapshotMode.parse("schema_only_recovery")).isEqualTo(SnapshotMode.SCHEMA_ONLY_RECOVERY);
        }

        @Test
        void shouldParseCaseInsensitive() {
            assertThat(SnapshotMode.parse("ALWAYS")).isEqualTo(SnapshotMode.ALWAYS);
            assertThat(SnapshotMode.parse("Always")).isEqualTo(SnapshotMode.ALWAYS);
            assertThat(SnapshotMode.parse("INITIAL_ONLY")).isEqualTo(SnapshotMode.INITIAL_ONLY);
            assertThat(SnapshotMode.parse("SCHEMA_ONLY_RECOVERY")).isEqualTo(SnapshotMode.SCHEMA_ONLY_RECOVERY);
        }

        @Test
        void shouldParseWithWhitespace() {
            assertThat(SnapshotMode.parse("  always  ")).isEqualTo(SnapshotMode.ALWAYS);
            assertThat(SnapshotMode.parse("\tinitial\n")).isEqualTo(SnapshotMode.INITIAL);
            assertThat(SnapshotMode.parse(" schema_only ")).isEqualTo(SnapshotMode.SCHEMA_ONLY);
        }

        @Test
        void shouldReturnNullForUnknownValue() {
            assertThat(SnapshotMode.parse("unknown")).isNull();
            assertThat(SnapshotMode.parse("foo")).isNull();
        }

        @Test
        void shouldReturnNullForNullInput() {
            assertThat(SnapshotMode.parse(null)).isNull();
        }

        @Test
        void shouldReportIncludeData() {
            assertThat(SnapshotMode.ALWAYS.includeData()).isTrue();
            assertThat(SnapshotMode.INITIAL.includeData()).isTrue();
            assertThat(SnapshotMode.INITIAL_ONLY.includeData()).isTrue();
            assertThat(SnapshotMode.SCHEMA_ONLY.includeData()).isFalse();
            assertThat(SnapshotMode.SCHEMA_ONLY_RECOVERY.includeData()).isFalse();
        }

        @Test
        void shouldReportShouldStream() {
            assertThat(SnapshotMode.ALWAYS.shouldStream()).isTrue();
            assertThat(SnapshotMode.INITIAL.shouldStream()).isTrue();
            assertThat(SnapshotMode.INITIAL_ONLY.shouldStream()).isFalse();
            assertThat(SnapshotMode.SCHEMA_ONLY.shouldStream()).isTrue();
            assertThat(SnapshotMode.SCHEMA_ONLY_RECOVERY.shouldStream()).isTrue();
        }

        @Test
        void shouldReportShouldSnapshotOnSchemaError() {
            assertThat(SnapshotMode.ALWAYS.shouldSnapshotOnSchemaError()).isTrue();
            assertThat(SnapshotMode.INITIAL.shouldSnapshotOnSchemaError()).isFalse();
            assertThat(SnapshotMode.INITIAL_ONLY.shouldSnapshotOnSchemaError()).isFalse();
            assertThat(SnapshotMode.SCHEMA_ONLY.shouldSnapshotOnSchemaError()).isFalse();
            assertThat(SnapshotMode.SCHEMA_ONLY_RECOVERY.shouldSnapshotOnSchemaError()).isTrue();
        }
    }

    // -----------------------------------------------------------------------
    // SnapshotLockingMode
    // -----------------------------------------------------------------------
    @Nested
    class SnapshotLockingModeTests {

        @Test
        void shouldHaveAllValues() {
            SnapshotLockingMode[] values = SnapshotLockingMode.values();
            assertThat(values).hasSize(2);
            assertThat(values).containsExactly(SnapshotLockingMode.SHARED, SnapshotLockingMode.NONE);
        }

        @Test
        void shouldReturnCorrectValues() {
            assertThat(SnapshotLockingMode.SHARED.getValue()).isEqualTo("shared");
            assertThat(SnapshotLockingMode.NONE.getValue()).isEqualTo("none");
        }

        @Test
        void shouldParseValidValue() {
            assertThat(SnapshotLockingMode.parse("shared")).isEqualTo(SnapshotLockingMode.SHARED);
            assertThat(SnapshotLockingMode.parse("none")).isEqualTo(SnapshotLockingMode.NONE);
        }

        @Test
        void shouldParseCaseInsensitive() {
            assertThat(SnapshotLockingMode.parse("SHARED")).isEqualTo(SnapshotLockingMode.SHARED);
            assertThat(SnapshotLockingMode.parse("Shared")).isEqualTo(SnapshotLockingMode.SHARED);
            assertThat(SnapshotLockingMode.parse("NONE")).isEqualTo(SnapshotLockingMode.NONE);
            assertThat(SnapshotLockingMode.parse("None")).isEqualTo(SnapshotLockingMode.NONE);
        }

        @Test
        void shouldParseWithWhitespace() {
            assertThat(SnapshotLockingMode.parse("  shared  ")).isEqualTo(SnapshotLockingMode.SHARED);
            assertThat(SnapshotLockingMode.parse("\tnone\n")).isEqualTo(SnapshotLockingMode.NONE);
        }

        @Test
        void shouldReturnNullForUnknownValue() {
            assertThat(SnapshotLockingMode.parse("unknown")).isNull();
            assertThat(SnapshotLockingMode.parse("foo")).isNull();
        }

        @Test
        void shouldReturnNullForNullInput() {
            assertThat(SnapshotLockingMode.parse(null)).isNull();
        }

        @Test
        void shouldReportUsesLocking() {
            assertThat(SnapshotLockingMode.SHARED.usesLocking()).isTrue();
            assertThat(SnapshotLockingMode.NONE.usesLocking()).isFalse();
        }
    }

    // -----------------------------------------------------------------------
    // TransactionSnapshotBoundaryMode
    // -----------------------------------------------------------------------
    @Nested
    class TransactionSnapshotBoundaryModeTests {

        @Test
        void shouldHaveAllValues() {
            TransactionSnapshotBoundaryMode[] values = TransactionSnapshotBoundaryMode.values();
            assertThat(values).hasSize(3);
            assertThat(values).containsExactly(
                    TransactionSnapshotBoundaryMode.SKIP,
                    TransactionSnapshotBoundaryMode.TRANSACTION_VIEW_ONLY,
                    TransactionSnapshotBoundaryMode.ALL);
        }

        @Test
        void shouldReturnCorrectValues() {
            assertThat(TransactionSnapshotBoundaryMode.SKIP.getValue()).isEqualTo("skip");
            assertThat(TransactionSnapshotBoundaryMode.TRANSACTION_VIEW_ONLY.getValue()).isEqualTo("transaction_view_only");
            assertThat(TransactionSnapshotBoundaryMode.ALL.getValue()).isEqualTo("all");
        }

        @Test
        void shouldParseValidValue() {
            assertThat(TransactionSnapshotBoundaryMode.parse("skip")).isEqualTo(TransactionSnapshotBoundaryMode.SKIP);
            assertThat(TransactionSnapshotBoundaryMode.parse("transaction_view_only")).isEqualTo(TransactionSnapshotBoundaryMode.TRANSACTION_VIEW_ONLY);
            assertThat(TransactionSnapshotBoundaryMode.parse("all")).isEqualTo(TransactionSnapshotBoundaryMode.ALL);
        }

        @Test
        void shouldParseCaseInsensitive() {
            assertThat(TransactionSnapshotBoundaryMode.parse("SKIP")).isEqualTo(TransactionSnapshotBoundaryMode.SKIP);
            assertThat(TransactionSnapshotBoundaryMode.parse("Skip")).isEqualTo(TransactionSnapshotBoundaryMode.SKIP);
            assertThat(TransactionSnapshotBoundaryMode.parse("TRANSACTION_VIEW_ONLY")).isEqualTo(TransactionSnapshotBoundaryMode.TRANSACTION_VIEW_ONLY);
            assertThat(TransactionSnapshotBoundaryMode.parse("ALL")).isEqualTo(TransactionSnapshotBoundaryMode.ALL);
        }

        @Test
        void shouldParseWithWhitespace() {
            assertThat(TransactionSnapshotBoundaryMode.parse("  skip  ")).isEqualTo(TransactionSnapshotBoundaryMode.SKIP);
            assertThat(TransactionSnapshotBoundaryMode.parse("\tall\n")).isEqualTo(TransactionSnapshotBoundaryMode.ALL);
            assertThat(TransactionSnapshotBoundaryMode.parse(" transaction_view_only ")).isEqualTo(TransactionSnapshotBoundaryMode.TRANSACTION_VIEW_ONLY);
        }

        @Test
        void shouldReturnNullForUnknownValue() {
            assertThat(TransactionSnapshotBoundaryMode.parse("unknown")).isNull();
            assertThat(TransactionSnapshotBoundaryMode.parse("foo")).isNull();
        }

        @Test
        void shouldReturnNullForNullInput() {
            assertThat(TransactionSnapshotBoundaryMode.parse(null)).isNull();
        }
    }

    // -----------------------------------------------------------------------
    // LogMiningStrategy
    // -----------------------------------------------------------------------
    @Nested
    class LogMiningStrategyTests {

        @Test
        void shouldHaveAllValues() {
            LogMiningStrategy[] values = LogMiningStrategy.values();
            assertThat(values).hasSize(2);
            assertThat(values).containsExactly(LogMiningStrategy.ONLINE_CATALOG, LogMiningStrategy.CATALOG_IN_REDO);
        }

        @Test
        void shouldReturnCorrectValues() {
            assertThat(LogMiningStrategy.ONLINE_CATALOG.getValue()).isEqualTo("online_catalog");
            assertThat(LogMiningStrategy.CATALOG_IN_REDO.getValue()).isEqualTo("redo_log_catalog");
        }

        @Test
        void shouldParseValidValue() {
            assertThat(LogMiningStrategy.parse("online_catalog")).isEqualTo(LogMiningStrategy.ONLINE_CATALOG);
            assertThat(LogMiningStrategy.parse("redo_log_catalog")).isEqualTo(LogMiningStrategy.CATALOG_IN_REDO);
        }

        @Test
        void shouldParseCaseInsensitive() {
            assertThat(LogMiningStrategy.parse("ONLINE_CATALOG")).isEqualTo(LogMiningStrategy.ONLINE_CATALOG);
            assertThat(LogMiningStrategy.parse("Online_Catalog")).isEqualTo(LogMiningStrategy.ONLINE_CATALOG);
            assertThat(LogMiningStrategy.parse("REDO_LOG_CATALOG")).isEqualTo(LogMiningStrategy.CATALOG_IN_REDO);
        }

        @Test
        void shouldParseWithWhitespace() {
            assertThat(LogMiningStrategy.parse("  online_catalog  ")).isEqualTo(LogMiningStrategy.ONLINE_CATALOG);
            assertThat(LogMiningStrategy.parse("\tredo_log_catalog\n")).isEqualTo(LogMiningStrategy.CATALOG_IN_REDO);
        }

        @Test
        void shouldReturnNullForUnknownValue() {
            assertThat(LogMiningStrategy.parse("unknown")).isNull();
            assertThat(LogMiningStrategy.parse("foo")).isNull();
        }

        @Test
        void shouldReturnNullForNullInput() {
            assertThat(LogMiningStrategy.parse(null)).isNull();
        }
    }

    // -----------------------------------------------------------------------
    // LogMiningQueryFilterMode
    // -----------------------------------------------------------------------
    @Nested
    class LogMiningQueryFilterModeTests {

        @Test
        void shouldHaveAllValues() {
            LogMiningQueryFilterMode[] values = LogMiningQueryFilterMode.values();
            assertThat(values).hasSize(3);
            assertThat(values).containsExactly(
                    LogMiningQueryFilterMode.NONE,
                    LogMiningQueryFilterMode.IN,
                    LogMiningQueryFilterMode.REGEX);
        }

        @Test
        void shouldReturnCorrectValues() {
            assertThat(LogMiningQueryFilterMode.NONE.getValue()).isEqualTo("none");
            assertThat(LogMiningQueryFilterMode.IN.getValue()).isEqualTo("in");
            assertThat(LogMiningQueryFilterMode.REGEX.getValue()).isEqualTo("regex");
        }

        @Test
        void shouldParseValidValue() {
            assertThat(LogMiningQueryFilterMode.parse("none")).isEqualTo(LogMiningQueryFilterMode.NONE);
            assertThat(LogMiningQueryFilterMode.parse("in")).isEqualTo(LogMiningQueryFilterMode.IN);
            assertThat(LogMiningQueryFilterMode.parse("regex")).isEqualTo(LogMiningQueryFilterMode.REGEX);
        }

        @Test
        void shouldParseCaseInsensitive() {
            assertThat(LogMiningQueryFilterMode.parse("NONE")).isEqualTo(LogMiningQueryFilterMode.NONE);
            assertThat(LogMiningQueryFilterMode.parse("None")).isEqualTo(LogMiningQueryFilterMode.NONE);
            assertThat(LogMiningQueryFilterMode.parse("IN")).isEqualTo(LogMiningQueryFilterMode.IN);
            assertThat(LogMiningQueryFilterMode.parse("REGEX")).isEqualTo(LogMiningQueryFilterMode.REGEX);
        }

        @Test
        void shouldParseWithWhitespace() {
            assertThat(LogMiningQueryFilterMode.parse("  none  ")).isEqualTo(LogMiningQueryFilterMode.NONE);
            assertThat(LogMiningQueryFilterMode.parse("\tin\n")).isEqualTo(LogMiningQueryFilterMode.IN);
            assertThat(LogMiningQueryFilterMode.parse(" regex ")).isEqualTo(LogMiningQueryFilterMode.REGEX);
        }

        @Test
        void shouldReturnNullForUnknownValue() {
            assertThat(LogMiningQueryFilterMode.parse("unknown")).isNull();
            assertThat(LogMiningQueryFilterMode.parse("foo")).isNull();
        }

        @Test
        void shouldReturnNullForNullInput() {
            assertThat(LogMiningQueryFilterMode.parse(null)).isNull();
        }
    }

    // -----------------------------------------------------------------------
    // Connector config-level constants and methods
    // -----------------------------------------------------------------------
    @Nested
    class ConnectorConfigTests {

        @Test
        void shouldHaveExcludedSchemas() {
            List<String> excludedSchemas = YashanDbConnectorConfig.EXCLUDED_SCHEMAS;
            assertThat(excludedSchemas).isNotNull();
            assertThat(excludedSchemas).isNotEmpty();
            assertThat(excludedSchemas).contains("SYS");
        }

        @Test
        void shouldHaveExcludedSchemasContainingMdsysAndXaSys() {
            List<String> excludedSchemas = YashanDbConnectorConfig.EXCLUDED_SCHEMAS;
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
        void shouldReturnNonNullConfigDef() {
            ConfigDef configDef = YashanDbConnectorConfig.configDef();
            assertThat(configDef).isNotNull();
        }
    }
}
