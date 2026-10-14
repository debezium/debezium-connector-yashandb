/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;

/**
 * Unit tests for {@link YashanDbConnection}.
 */
class YashanDbConnectionTest {

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldIgnoreDatabaseNameInNativeTableId() throws SQLException {
        try (YashanDbConnection connection = new YashanDbConnection(JdbcConfiguration.create().build())) {
            assertThat(connection.createTableId("SYS", "DBZ", "DBZ4321"))
                    .isEqualTo(new TableId("", "DBZ", "DBZ4321"));
        }
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldIgnoreDatabaseNameInMySqlTableId() throws SQLException {
        try (YashanDbConnection connection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(true))) {
            assertThat(connection.createTableId("SYS", "DBZ", "DBZ4321"))
                    .isEqualTo(new TableId("", "DBZ", "DBZ4321"));
        }
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldReadMysqlTimeAsStringForSnapshot() throws SQLException {
        JdbcConfiguration config = JdbcConfiguration.create()
                .withHostname("localhost")
                .withPort(1688)
                .withDatabase("yashandb")
                .build();
        ResultSet resultSet = mock(ResultSet.class);
        Column column = Column.editor().name("duration").type("time").jdbcType(Types.TIME).create();
        when(resultSet.getString(1)).thenReturn("838:59:59");

        try (YashanDbConnection connection = new YashanDbConnection(config, new YashanDbModeContext(true))) {
            assertThat(connection.getColumnValue(resultSet, 1, column, null)).isEqualTo("838:59:59");
        }
        verify(resultSet).getString(1);
    }

    @FixFor("debezium/dbz#3014")
    @ParameterizedTest
    @CsvSource({ "true,false,", "true,true,", "true,false,yashan", "false,false,", "false,true,", "false,false,yashan" })
    void shouldPassCompatibilityPropertyToDriver(boolean mysqlMode, boolean useRawUrl, String configuredCompatibility) throws SQLException {
        JdbcConfiguration.Builder builder = JdbcConfiguration.create()
                .withHostname("localhost")
                .withPort(1688)
                .withDatabase("yashandb")
                .withUser("test_user")
                .with("customProperty", "custom_value");
        String url = useRawUrl ? "jdbc:yasdb://db.example.com:5521/mydb" : "jdbc:yasdb://localhost:1688/yashandb";
        if (useRawUrl) {
            builder.with("url", url);
        }
        if (configuredCompatibility != null) {
            builder.with("compatVector", configuredCompatibility);
        }
        JdbcConfiguration config = builder.build();
        Connection jdbcConnection = mock(Connection.class);

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class);
                YashanDbConnection connection = new YashanDbConnection(config, new YashanDbModeContext(mysqlMode))) {
            driverManager.when(() -> DriverManager.getConnection(eq(url), any(Properties.class))).thenReturn(jdbcConnection);

            connection.connect();

            ArgumentCaptor<Properties> properties = ArgumentCaptor.forClass(Properties.class);
            driverManager.verify(() -> DriverManager.getConnection(eq(url), properties.capture()));
            assertThat(properties.getValue().getProperty("compatVector")).isEqualTo(mysqlMode ? "mysql" : configuredCompatibility);
            assertThat(properties.getValue().getProperty("user")).isEqualTo("test_user");
            assertThat(properties.getValue().getProperty("customProperty")).isEqualTo("custom_value");
            assertThat(config.getString("compatVector")).isEqualTo(configuredCompatibility);
        }
    }

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
        YashanDbConnection connection = nativeConnection();

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

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldUseSelectedDialectForLimitedSelect() {
        TableId nativeTable = new TableId("", "APP", "ORDERS");
        String nativeSql = nativeConnection().buildSelectWithRowLimits(nativeTable, 10, "ID",
                Optional.of("ID > 0"), Optional.empty(), "ID");
        assertThat(nativeSql).isEqualTo(" SELECT * FROM (SELECT ID FROM \"APP\".\"ORDERS\" WHERE ID > 0 ORDER BY ID) WHERE ROWNUM <=10");

        JdbcConfiguration config = JdbcConfiguration.create()
                .withHostname("localhost")
                .withPort(1688)
                .withDatabase("yashandb")
                .build();
        YashanDbConnection mysqlConnection = new YashanDbConnection(config, new YashanDbModeContext(true));
        TableId mysqlTable = new TableId(null, "inventory", "orders");
        String mysqlSql = mysqlConnection.buildSelectWithRowLimits(mysqlTable, 10, "ID",
                Optional.of("ID > 0"), Optional.empty(), "ID");
        String quote = String.valueOf((char) 96);
        assertThat(mysqlSql).isEqualTo("SELECT ID FROM " + quote + "inventory" + quote + "." + quote + "orders" + quote
                + " WHERE ID > 0 ORDER BY ID LIMIT 10");
    }

    @Test
    void shouldBuildSelectWithRowLimitsWithCondition() {
        YashanDbConnection connection = nativeConnection();

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
        YashanDbConnection connection = nativeConnection();

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
        YashanDbConnection connection = nativeConnection();

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
        YashanDbConnection connection = nativeConnection();

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

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldRejectNullModeContext() {
        JdbcConfiguration config = JdbcConfiguration.create()
                .withHostname("localhost")
                .withPort(1688)
                .withDatabase("yashandb")
                .build();
        assertThatThrownBy(() -> new YashanDbConnection(config, (YashanDbModeContext) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("modeContext");
    }

    private static YashanDbConnection nativeConnection() {
        return new YashanDbConnection(JdbcConfiguration.create()
                .withHostname("localhost")
                .withPort(1688)
                .withDatabase("yashandb")
                .build());
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
