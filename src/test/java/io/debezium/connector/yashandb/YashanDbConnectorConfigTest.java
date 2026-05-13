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
import io.debezium.connector.yashandb.YashanDbConnectorConfig.SnapshotLockingMode;
import io.debezium.connector.yashandb.YashanDbConnectorConfig.SnapshotMode;

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
                    SnapshotMode.INITIAL_ONLY);
        }

        @Test
        void shouldReturnCorrectValues() {
            assertThat(SnapshotMode.ALWAYS.getValue()).isEqualTo("always");
            assertThat(SnapshotMode.INITIAL.getValue()).isEqualTo("initial");
            assertThat(SnapshotMode.INITIAL_ONLY.getValue()).isEqualTo("initial_only");
        }

        @Test
        void shouldParseValidValue() {
            assertThat(SnapshotMode.parse("always")).isEqualTo(SnapshotMode.ALWAYS);
            assertThat(SnapshotMode.parse("initial")).isEqualTo(SnapshotMode.INITIAL);
            assertThat(SnapshotMode.parse("initial_only")).isEqualTo(SnapshotMode.INITIAL_ONLY);
        }

        @Test
        void shouldParseCaseInsensitive() {
            assertThat(SnapshotMode.parse("ALWAYS")).isEqualTo(SnapshotMode.ALWAYS);
            assertThat(SnapshotMode.parse("Always")).isEqualTo(SnapshotMode.ALWAYS);
            assertThat(SnapshotMode.parse("INITIAL_ONLY")).isEqualTo(SnapshotMode.INITIAL_ONLY);
        }

        @Test
        void shouldParseWithWhitespace() {
            assertThat(SnapshotMode.parse("  always  ")).isEqualTo(SnapshotMode.ALWAYS);
            assertThat(SnapshotMode.parse("\tinitial\n")).isEqualTo(SnapshotMode.INITIAL);
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
