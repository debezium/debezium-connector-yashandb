/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.TableId;

/**
 * Unit tests for {@link YashanDbConnection}.
 */
class YashanDbConnectionTest {

    // -----------------------------------------------------------------------
    // connectionString tests
    // -----------------------------------------------------------------------

    @Test
    void shouldBuildConnectionStringWithDefaultFormat() {
        JdbcConfiguration config = JdbcConfiguration.create()
                .withHostname("localhost")
                .withPort(1688)
                .withDatabase("yashandb")
                .build();

        String connStr = YashanDbConnection.connectionString(config);

        assertThat(connStr).isEqualTo("jdbc:yasdb://localhost:1688/yashandb");
    }

    @Test
    void shouldBuildConnectionStringWithCustomPort() {
        JdbcConfiguration config = JdbcConfiguration.create()
                .withHostname("db.example.com")
                .withPort(5521)
                .withDatabase("mydb")
                .build();

        String connStr = YashanDbConnection.connectionString(config);

        assertThat(connStr).isEqualTo("jdbc:yasdb://db.example.com:5521/mydb");
    }

    // -----------------------------------------------------------------------
    // buildSelectWithRowLimits tests
    // -----------------------------------------------------------------------

    @Test
    void shouldBuildSelectWithRowLimitsNoCondition() {
        YashanDbConnection connection = mock(YashanDbConnection.class,
                withSettings().defaultAnswer(CALLS_REAL_METHODS));

        TableId tableId = new TableId(null, "SCHEMA", "TABLE_NAME");
        String sql = connection.buildSelectWithRowLimits(
                tableId,
                100,
                "*",
                Optional.empty(),
                Optional.empty(),
                "ID");

        assertThat(sql).contains("SELECT * FROM (");
        assertThat(sql).contains("SELECT * FROM \"SCHEMA\".\"TABLE_NAME\"");
        assertThat(sql).contains("ORDER BY ID");
        assertThat(sql).contains("ROWNUM <=");
        assertThat(sql).contains("100");
        // ROWNUM uses WHERE clause, but no user-defined condition
        assertThat(sql).contains("WHERE ROWNUM <=");
    }

    @Test
    void shouldBuildSelectWithRowLimitsWithCondition() {
        YashanDbConnection connection = mock(YashanDbConnection.class,
                withSettings().defaultAnswer(CALLS_REAL_METHODS));

        TableId tableId = new TableId(null, "SCHEMA", "TABLE_NAME");
        String sql = connection.buildSelectWithRowLimits(
                tableId,
                50,
                "ID, NAME",
                Optional.of("ID > 100"),
                Optional.empty(),
                "CREATE_TIME");

        assertThat(sql).contains("WHERE ID > 100");
        assertThat(sql).contains("ORDER BY CREATE_TIME");
        assertThat(sql).contains("50");
        assertThat(sql).contains("SELECT ID, NAME FROM");
    }

    @Test
    void shouldBuildSelectWithRowLimitsWithAdditionalConditionOnly() {
        YashanDbConnection connection = mock(YashanDbConnection.class,
                withSettings().defaultAnswer(CALLS_REAL_METHODS));

        TableId tableId = new TableId(null, "SCHEMA", "TABLE_NAME");
        String sql = connection.buildSelectWithRowLimits(
                tableId,
                25,
                "*",
                Optional.empty(),
                Optional.of("STATUS = 'ACTIVE'"),
                "ID");

        assertThat(sql).contains("WHERE STATUS = 'ACTIVE'");
        assertThat(sql).contains("25");
        // ROWNUM WHERE clause present, but no AND since only additionalCondition
        assertThat(sql).contains("WHERE ROWNUM <=");
    }

    @Test
    void shouldBuildSelectWithRowLimitsWithBothConditions() {
        YashanDbConnection connection = mock(YashanDbConnection.class,
                withSettings().defaultAnswer(CALLS_REAL_METHODS));

        TableId tableId = new TableId(null, "SCHEMA", "TABLE_NAME");
        String sql = connection.buildSelectWithRowLimits(
                tableId,
                10,
                "*",
                Optional.of("ID > 0"),
                Optional.of("DELETED = 0"),
                "ID");

        assertThat(sql).contains("WHERE ID > 0 AND DELETED = 0");
    }

    @Test
    void shouldBuildSelectWithRowLimitsWithLargeLimit() {
        YashanDbConnection connection = mock(YashanDbConnection.class,
                withSettings().defaultAnswer(CALLS_REAL_METHODS));

        TableId tableId = new TableId(null, "OWNER", "BIG_TABLE");
        String sql = connection.buildSelectWithRowLimits(
                tableId,
                1000000,
                "COL1, COL2, COL3",
                Optional.empty(),
                Optional.empty(),
                "ROWID");

        assertThat(sql).contains("ROWNUM <=");
        assertThat(sql).contains("1000000");
    }

    // -----------------------------------------------------------------------
    // TableId tests
    // -----------------------------------------------------------------------

    @Test
    void shouldConstructTableIdWithCatalog() {
        TableId tableId = new TableId("catalog", "schema", "table");
        assertThat(tableId.catalog()).isEqualTo("catalog");
        assertThat(tableId.schema()).isEqualTo("schema");
        assertThat(tableId.table()).isEqualTo("table");
    }

    @Test
    void shouldConstructTableIdWithEmptyCatalog() {
        TableId tableId = new TableId("", "MYSCHEMA", "MYTABLE");
        assertThat(tableId.catalog()).isEmpty();
        assertThat(tableId.schema()).isEqualTo("MYSCHEMA");
        assertThat(tableId.table()).isEqualTo("MYTABLE");
    }

    // -----------------------------------------------------------------------
    // Scn tests
    // -----------------------------------------------------------------------

    @Test
    void shouldParseScnValue() {
        Scn scn = Scn.valueOf(12345L);
        assertThat(scn.longValue()).isEqualTo(12345L);
    }

    @Test
    void shouldParseScnFromString() {
        Scn scn = Scn.valueOf("12345");
        assertThat(scn.longValue()).isEqualTo(12345L);
    }

    @Test
    void shouldHandleNullScn() {
        Scn scn = Scn.NULL;
        assertThat(scn.isNull()).isTrue();
    }
}
