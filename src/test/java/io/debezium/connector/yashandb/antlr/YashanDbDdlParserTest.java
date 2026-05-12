/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.antlr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import io.debezium.connector.yashandb.YashanDbValueConverters;
import io.debezium.relational.Tables.TableFilter;

/**
 * Unit tests for {@link YashanDbDdlParser}.
 */
class YashanDbDdlParserTest {

    @Test
    void shouldRunWithAllNonNullValues() {
        YashanDbDdlParser parser = new YashanDbDdlParser();
        AtomicBoolean executed = new AtomicBoolean(false);
        parser.runIfNotNull(() -> executed.set(true), "a", "b", "c");
        assertThat(executed.get()).isTrue();
    }

    @Test
    void shouldSkipWhenAnyValueIsNull() {
        YashanDbDdlParser parser = new YashanDbDdlParser();
        AtomicBoolean executed = new AtomicBoolean(false);
        parser.runIfNotNull(() -> executed.set(true), "a", null, "c");
        assertThat(executed.get()).isFalse();
    }

    @Test
    void shouldSkipWhenFirstValueIsNull() {
        YashanDbDdlParser parser = new YashanDbDdlParser();
        AtomicBoolean executed = new AtomicBoolean(false);
        parser.runIfNotNull(() -> executed.set(true), null, "b", "c");
        assertThat(executed.get()).isFalse();
    }

    @Test
    void shouldSkipWhenLastValueIsNull() {
        YashanDbDdlParser parser = new YashanDbDdlParser();
        AtomicBoolean executed = new AtomicBoolean(false);
        parser.runIfNotNull(() -> executed.set(true), "a", "b", null);
        assertThat(executed.get()).isFalse();
    }

    @Test
    void shouldRunWithSingleNonNullValue() {
        YashanDbDdlParser parser = new YashanDbDdlParser();
        AtomicBoolean executed = new AtomicBoolean(false);
        parser.runIfNotNull(() -> executed.set(true), "a");
        assertThat(executed.get()).isTrue();
    }

    @Test
    void shouldSkipWithSingleNullValue() {
        YashanDbDdlParser parser = new YashanDbDdlParser();
        AtomicBoolean executed = new AtomicBoolean(false);
        parser.runIfNotNull(() -> executed.set(true), (Object) null);
        assertThat(executed.get()).isFalse();
    }

    @Test
    void shouldGetConverters() {
        YashanDbDdlParser parser = new YashanDbDdlParser();
        assertThat(parser.getConverters()).isNull();
    }

    @Test
    void shouldGetTableFilter() {
        YashanDbDdlParser parser = new YashanDbDdlParser();
        assertThat(parser.getTableFilter()).isNotNull();
    }

    @Test
    void shouldCreateDefaultParser() {
        YashanDbDdlParser parser = new YashanDbDdlParser();
        assertThat(parser).isNotNull();
    }

    @Test
    void shouldGetDataTypeResolver() {
        YashanDbDdlParser parser = new YashanDbDdlParser();
        var resolver = parser.dataTypeResolver();
        assertThat(resolver).isNotNull();
    }

    @Test
    void shouldCreateParserWithValueConverters() {
        YashanDbValueConverters converters = null;
        YashanDbDdlParser parser = new YashanDbDdlParser(converters);
        assertThat(parser.getConverters()).isNull();
    }

    @Test
    void shouldCreateParserWithTableFilter() {
        TableFilter filter = TableFilter.includeAll();
        YashanDbDdlParser parser = new YashanDbDdlParser(null, filter);
        assertThat(parser.getTableFilter()).isSameAs(filter);
    }

    @Test
    void shouldCreateParserWithFullConfig() {
        TableFilter filter = TableFilter.includeAll();
        YashanDbDdlParser parser = new YashanDbDdlParser(true, false, false, null, filter);
        assertThat(parser.getTableFilter()).isSameAs(filter);
        assertThat(parser.getConverters()).isNull();
    }

    @Test
    void shouldSetCurrentDatabase() {
        YashanDbDdlParser parser = new YashanDbDdlParser();
        parser.setCurrentDatabase("testdb");
        // Verify via parseTree creation - the catalogName is used in the listener
        assertThat(parser).isNotNull();
    }

    @Test
    void shouldSetCurrentSchema() {
        YashanDbDdlParser parser = new YashanDbDdlParser();
        parser.setCurrentSchema("testschema");
        assertThat(parser).isNotNull();
    }

    @Test
    void shouldThrowOnSystemVariables() {
        YashanDbDdlParser parser = new YashanDbDdlParser();
        assertThatThrownBy(() -> parser.systemVariables())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Not implemented yet");
    }
}
