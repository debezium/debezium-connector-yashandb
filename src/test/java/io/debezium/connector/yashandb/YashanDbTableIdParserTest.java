/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

/**
 * Unit tests for {@link YashanDbTableIdParser}.
 */
class YashanDbTableIdParserTest {

    @Test
    void shouldParseSimpleTableId() {
        TableId tableId = YashanDbTableIdParser.parse("schema.table");
        assertThat(tableId.schema()).isEqualTo("schema");
        assertThat(tableId.table()).isEqualTo("table");
        assertThat(tableId.catalog()).isNull();
    }

    @Test
    void shouldParseTableIdWithCatalog() {
        TableId tableId = YashanDbTableIdParser.parse("catalog.schema.table");
        assertThat(tableId.catalog()).isEqualTo("catalog");
        assertThat(tableId.schema()).isEqualTo("schema");
        assertThat(tableId.table()).isEqualTo("table");
    }

    @Test
    void shouldParseTableIdWithDomain() {
        // YashanDB: domain.schema.table where domain may have dots
        TableId tableId = YashanDbTableIdParser.parse("domain.sub.schema.table");
        assertThat(tableId.catalog()).isEqualTo("domain.sub");
        assertThat(tableId.schema()).isEqualTo("schema");
        assertThat(tableId.table()).isEqualTo("table");
    }

    @Test
    void shouldParseTableIdWithComplexDomain() {
        TableId tableId = YashanDbTableIdParser.parse("a.b.c.schema.table");
        assertThat(tableId.catalog()).isEqualTo("a.b.c");
        assertThat(tableId.schema()).isEqualTo("schema");
        assertThat(tableId.table()).isEqualTo("table");
    }

    @Test
    void shouldQuoteIfNeededWithUnderscorePrefix() {
        TableId tableId = new TableId(null, "_schema", "table");
        List<String> keywords = Collections.emptyList();
        String result = YashanDbTableIdParser.quoteIfNeeded(tableId, false, true, keywords);
        assertThat(result).isEqualTo("\"_schema\".table");
    }

    @Test
    void shouldQuoteIfNeededWithKeyword() {
        TableId tableId = new TableId(null, "SELECT", "table");
        List<String> keywords = Arrays.asList("SELECT", "FROM", "WHERE");
        String result = YashanDbTableIdParser.quoteIfNeeded(tableId, false, true, keywords);
        assertThat(result).isEqualTo("\"SELECT\".table");
    }

    @Test
    void shouldNotQuoteNormalName() {
        TableId tableId = new TableId(null, "normal", "users");
        List<String> keywords = Arrays.asList("SELECT", "FROM");
        String result = YashanDbTableIdParser.quoteIfNeeded(tableId, false, true, keywords);
        assertThat(result).isEqualTo("normal.users");
    }

    @Test
    void shouldQuoteWithCatalog() {
        TableId tableId = new TableId("catalog", "schema", "table");
        List<String> keywords = Collections.emptyList();
        String result = YashanDbTableIdParser.quoteIfNeeded(tableId, true, false, keywords);
        assertThat(result).isEqualTo("catalog.table");
    }

    @Test
    void shouldQuoteCatalogKeyword() {
        TableId tableId = new TableId("SELECT", "schema", "table");
        List<String> keywords = Arrays.asList("SELECT");
        String result = YashanDbTableIdParser.quoteIfNeeded(tableId, true, false, keywords);
        assertThat(result).isEqualTo("\"SELECT\".table");
    }

    @Test
    void shouldHandleNullPart() {
        TableId tableId = new TableId(null, null, "table");
        List<String> keywords = Collections.emptyList();
        String result = YashanDbTableIdParser.quoteIfNeeded(tableId, false, true, keywords);
        // null schema part produces "null.table" in the output
        assertThat(result).isEqualTo("null.table");
    }

    @Test
    void shouldHandleEmptyPart() {
        TableId tableId = new TableId(null, "", "table");
        List<String> keywords = Collections.emptyList();
        String result = YashanDbTableIdParser.quoteIfNeeded(tableId, false, true, keywords);
        assertThat(result).isEqualTo(".table");
    }

    @Test
    void shouldIgnoreCaseMatching() {
        TableId tableId = new TableId(null, "select", "table");
        List<String> keywords = Arrays.asList("SELECT");
        String result = YashanDbTableIdParser.quoteIfNeeded(tableId, false, true, keywords);
        assertThat(result).isEqualTo("\"select\".table");
    }
}
