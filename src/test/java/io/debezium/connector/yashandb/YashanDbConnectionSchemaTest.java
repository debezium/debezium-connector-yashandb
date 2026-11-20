/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.connector.yashandb.antlr.YashanDbMySqlDdlParser;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class YashanDbConnectionSchemaTest {

    private DatabaseMetaData metadata;
    private Connection jdbcConnection;
    private YashanDbConnection connection;
    private Tables tables;

    @BeforeEach
    void setUp() throws SQLException {
        metadata = mock(DatabaseMetaData.class);
        jdbcConnection = mock(Connection.class);
        when(jdbcConnection.getMetaData()).thenReturn(metadata);
        when(metadata.getSearchStringEscape()).thenReturn("\\");
        ResultSet emptyResult = resultSet();
        PreparedStatement emptyStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(anyString())).thenReturn(emptyStatement);
        doReturn(emptyResult).when(emptyStatement).executeQuery();
        // Reproduce the driver's failure when only a schema is supplied.
        when(metadata.getColumns(any(), any(), isNull(), any()))
                .thenThrow(new SQLSyntaxErrorException("YAS-04323 arguments count must be 2"));
        doReturn(emptyResult).when(metadata).getPrimaryKeys(any(), any(), any());
        doReturn(emptyResult).when(metadata).getIndexInfo(any(), any(), any(), anyBoolean(), anyBoolean());
        connection = new YashanDbConnection(JdbcConfiguration.create().build(), config -> jdbcConnection);
        tables = new Tables();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldKeepUsingJdbcMetadataForNativeModeColumns() throws SQLException {
        ResultSet tableMetadata = resultSet(new Object[]{ null, "APP", "ORDERS", "TABLE" });
        ResultSet columnMetadata = resultSet(new Object[]{ null, "APP", "ORDERS", "ID", Types.INTEGER, "INTEGER", 10, null, 0,
                10, DatabaseMetaData.columnNoNulls, null, null, null, null, null, 1, "NO", null, null, null, null, null, null });
        when(metadata.getTables(any(), any(), any(), any())).thenReturn(tableMetadata);
        doReturn(columnMetadata).when(metadata).getColumns(any(), eq("APP"), isNull(), any());

        connection.readSchema(tables, null, "APP", null, null, false);

        Table orders = tables.forTable(tableId("ORDERS"));
        assertThat(orders.columnWithName("ID").jdbcType()).isEqualTo(Types.INTEGER);
        assertThat(orders.columnWithName("ID").length()).isEqualTo(10);
        assertThat(orders.columnWithName("ID").typeName()).contains("INTEGER");
        verify(metadata).getColumns(any(), eq("APP"), isNull(), any());
        verify(jdbcConnection, never()).prepareStatement(contains("all_tab_cols"));
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldUseSameMysqlJdbcTypesForSnapshotAndShowCreateTableSchema() throws SQLException {
        YashanDbConnection mysqlConnection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(true)) {
            @Override
            public Connection connection() {
                return YashanDbConnectionSchemaTest.this.jdbcConnection;
            }
        };
        PreparedStatement tablesStatement = mock(PreparedStatement.class);
        PreparedStatement columnsStatement = mock(PreparedStatement.class);
        PreparedStatement keysStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(contains("information_schema.tables"))).thenReturn(tablesStatement);
        when(jdbcConnection.prepareStatement(contains("information_schema.columns"))).thenReturn(columnsStatement);
        when(jdbcConnection.prepareStatement(contains("information_schema.statistics"))).thenReturn(keysStatement);
        doReturn(resultSet(new Object[]{ "APP", "TYPE_SAMPLES" })).when(tablesStatement).executeQuery();
        doReturn(resultSet(
                mysqlColumn("APP", "TYPE_SAMPLES", "TINYTEXT_COL", "tinytext", 255, null, null, "YES", null, 1, null),
                mysqlColumn("APP", "TYPE_SAMPLES", "TEXT_COL", "text", 65535, null, null, "YES", null, 2, null),
                mysqlColumn("APP", "TYPE_SAMPLES", "MEDIUMTEXT_COL", "mediumtext", 16777215, null, null, "YES", null, 3, null),
                mysqlColumn("APP", "TYPE_SAMPLES", "LONGTEXT_COL", "longtext", Integer.MAX_VALUE, null, null, "YES", null, 4, null),
                mysqlColumn("APP", "TYPE_SAMPLES", "ENUM_COL", "enum('A','B')", 1, null, null, "YES", null, 5, null),
                mysqlColumn("APP", "TYPE_SAMPLES", "SET_COL", "set('A','B')", 3, null, null, "YES", null, 6, null),
                mysqlColumn("APP", "TYPE_SAMPLES", "CHAR_COL", "char(1)", 1, null, null, "YES", null, 7, null)))
                .when(columnsStatement).executeQuery();
        doReturn(resultSet()).when(keysStatement).executeQuery();

        mysqlConnection.readSchema(tables, null, "APP", null, null, false);

        Tables ddlTables = new Tables();
        new YashanDbMySqlDdlParser().parse("CREATE TABLE `APP`.`TYPE_SAMPLES` ("
                + "`TINYTEXT_COL` TINYTEXT, `TEXT_COL` TEXT, `MEDIUMTEXT_COL` MEDIUMTEXT, `LONGTEXT_COL` LONGTEXT, "
                + "`ENUM_COL` ENUM('A','B'), `SET_COL` SET('A','B'), `CHAR_COL` CHAR(1))", ddlTables);
        Table snapshotTable = tables.forTable(new TableId(null, "APP", "TYPE_SAMPLES"));
        Table ddlTable = ddlTables.forTable(new TableId(null, "APP", "TYPE_SAMPLES"));

        assertThat(snapshotTable.columns()).extracting(Column::jdbcType)
                .containsExactlyElementsOf(ddlTable.columns().stream().map(Column::jdbcType).collect(Collectors.toList()));
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldReadMysqlPrimaryAndUniqueKeysForTablesInOneBatchQuery() throws SQLException {
        YashanDbConnection mysqlConnection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(true)) {
            @Override
            public Connection connection() {
                return YashanDbConnectionSchemaTest.this.jdbcConnection;
            }
        };
        PreparedStatement tablesStatement = mock(PreparedStatement.class);
        PreparedStatement columnsStatement = mock(PreparedStatement.class);
        PreparedStatement uniqueIndexStatement = mock(PreparedStatement.class);
        PreparedStatement keysStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(contains("information_schema.tables"))).thenReturn(tablesStatement);
        when(jdbcConnection.prepareStatement(contains("information_schema.statistics"))).thenReturn(uniqueIndexStatement);
        when(jdbcConnection.prepareStatement(contains("table_name IN"))).thenReturn(keysStatement);
        when(jdbcConnection.prepareStatement(contains("information_schema.columns"))).thenReturn(columnsStatement);
        doReturn(resultSet(new Object[]{ "APP", "ORDERS" }, new Object[]{ "APP", "CUSTOMERS" }))
                .when(tablesStatement).executeQuery();
        doReturn(resultSet()).when(uniqueIndexStatement).executeQuery();
        doReturn(resultSet(
                mysqlColumn("APP", "ORDERS", "REGION", "varchar", 16, null, null, "NO", null, 1, null),
                mysqlColumn("APP", "ORDERS", "ID", "bigint", null, 19, 0, "NO", null, 2, null),
                mysqlColumn("APP", "CUSTOMERS", "EMAIL", "varchar", 100, null, null, "NO", null, 1, null)))
                .when(columnsStatement).executeQuery();
        doReturn(resultSet(
                new Object[]{ "APP", "ORDERS", "PRIMARY", "P", "REGION", 1 },
                new Object[]{ "APP", "ORDERS", "PRIMARY", "P", "ID", 2 },
                new Object[]{ "APP", "CUSTOMERS", "CUSTOMERS_EMAIL_UK", "U", "EMAIL", 1 }))
                .when(keysStatement).executeQuery();

        mysqlConnection.readSchema(tables, null, "APP", null, null, false);

        TableId orders = new TableId(null, "APP", "ORDERS");
        TableId customers = new TableId(null, "APP", "CUSTOMERS");
        assertThat(tables.forTable(orders).primaryKeyColumnNames()).containsExactly("REGION", "ID");
        assertThat(tables.forTable(customers).primaryKeyColumnNames()).containsExactly("EMAIL");
        verify(keysStatement).setString(1, "APP");
        verify(keysStatement).setString(2, "ORDERS");
        verify(keysStatement).setString(3, "CUSTOMERS");
        ArgumentCaptor<String> queries = ArgumentCaptor.forClass(String.class);
        verify(jdbcConnection, org.mockito.Mockito.atLeastOnce()).prepareStatement(queries.capture());
        assertThat(queries.getAllValues()).anyMatch(sql -> sql.contains("information_schema.statistics")
                && sql.contains("table_name IN") && !sql.contains("information_schema.key_column_usage"));
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldUseSchemaForMysqlModeMetadata() throws SQLException {
        YashanDbConnection mysqlConnection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(true)) {
            @Override
            public Connection connection() {
                return YashanDbConnectionSchemaTest.this.jdbcConnection;
            }
        };
        PreparedStatement tablesStatement = mock(PreparedStatement.class);
        PreparedStatement columnsStatement = mock(PreparedStatement.class);
        PreparedStatement keyStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(contains("information_schema.tables"))).thenReturn(tablesStatement);
        when(jdbcConnection.prepareStatement(contains("information_schema.columns"))).thenReturn(columnsStatement);
        when(jdbcConnection.prepareStatement(contains("information_schema.statistics"))).thenReturn(keyStatement);
        doReturn(resultSet(new Object[]{ "APP", "ORDERS" })).when(tablesStatement).executeQuery();
        doReturn(resultSet(mysqlColumn("APP", "ORDERS", "ID", "bigint(20) unsigned", null, 19, 0, "NO", null, 1, "auto_increment"),
                mysqlColumn("APP", "ORDERS", "DESCRIPTION", "varchar(255)", 255, null, null, "YES", "'new'", 2, null)))
                .when(columnsStatement).executeQuery();
        doReturn(resultSet(new Object[]{ "APP", "ORDERS", "PRIMARY", "P", "ID", 1 })).when(keyStatement).executeQuery();

        mysqlConnection.readSchema(tables, null, "APP", null, null, false);

        TableId orders = new TableId(null, "APP", "ORDERS");
        assertThat(mysqlConnection.createTableId("APP", null, "ORDERS")).isEqualTo(new TableId(null, null, "ORDERS"));
        assertThat(tables.tableIds()).containsExactly(orders);
        assertThat(tables.forTable(orders).columns()).extracting(Column::name).containsExactly("ID", "DESCRIPTION");
        assertThat(tables.forTable(orders).columnWithName("ID").jdbcType()).isEqualTo(Types.BIGINT);
        assertThat(tables.forTable(orders).columnWithName("ID").typeName()).isEqualTo("bigint(20) unsigned");
        assertThat(tables.forTable(orders).columnWithName("ID").isAutoIncremented()).isTrue();
        assertThat(tables.forTable(orders).columnWithName("ID").isOptional()).isFalse();
        assertThat(tables.forTable(orders).columnWithName("ID").scale()).contains(0);
        assertThat(tables.forTable(orders).columnWithName("DESCRIPTION").length()).isEqualTo(255);
        assertThat(tables.forTable(orders).columnWithName("DESCRIPTION").jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(tables.forTable(orders).columnWithName("DESCRIPTION").isOptional()).isTrue();
        assertThat(tables.forTable(orders).columnWithName("DESCRIPTION").defaultValueExpression()).contains("'new'");
        assertThat(tables.forTable(orders).primaryKeyColumnNames()).containsExactly("ID");
        verify(metadata, never()).getTables(any(), any(), any(), any());
        verify(metadata, never()).getColumns(any(), any(), any(), any());
        verify(metadata, never()).getPrimaryKeys(any(), any(), any());
        verify(metadata, never()).getIndexInfo(any(), any(), any(), anyBoolean(), anyBoolean());
        verify(jdbcConnection).prepareStatement(contains("column_name, column_type"));
        verify(jdbcConnection).prepareStatement(contains("information_schema.tables"));
        verify(jdbcConnection).prepareStatement(contains("information_schema.statistics"));
        verify(tablesStatement).setString(1, "APP");
        verify(columnsStatement).setString(1, "APP");
        verify(columnsStatement).setString(2, "ORDERS");
        verify(keyStatement).setString(1, "APP");
        verify(keyStatement).setString(2, "ORDERS");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldApplyColumnFilterToMysqlSqlMetadata() throws SQLException {
        YashanDbConnection mysqlConnection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(true)) {
            @Override
            public Connection connection() {
                return YashanDbConnectionSchemaTest.this.jdbcConnection;
            }
        };
        PreparedStatement tablesStatement = mock(PreparedStatement.class);
        PreparedStatement columnsStatement = mock(PreparedStatement.class);
        PreparedStatement uniqueIndexStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(contains("information_schema.tables"))).thenReturn(tablesStatement);
        when(jdbcConnection.prepareStatement(contains("information_schema.columns"))).thenReturn(columnsStatement);
        when(jdbcConnection.prepareStatement(contains("information_schema.statistics"))).thenReturn(uniqueIndexStatement);
        doReturn(resultSet(new Object[]{ "APP", "ORDERS" })).when(tablesStatement).executeQuery();
        doReturn(resultSet(mysqlColumn("APP", "ORDERS", "ID", "bigint", null, 19, 0, "NO", null, 1, null),
                mysqlColumn("APP", "ORDERS", "SECRET", "varchar", 100, null, null, "YES", null, 2, null)))
                .when(columnsStatement).executeQuery();
        doReturn(resultSet()).when(uniqueIndexStatement).executeQuery();

        mysqlConnection.readSchema(tables, null, "APP", null, (catalog, schema, table, column) -> !column.equals("SECRET"), false);

        TableId orders = new TableId(null, "APP", "ORDERS");
        assertThat(tables.forTable(orders).columns()).extracting(Column::name).containsExactly("ID");
        verify(metadata, never()).getTables(any(), any(), any(), any());
        verify(metadata, never()).getColumns(any(), any(), any(), any());
        verify(metadata, never()).getPrimaryKeys(any(), any(), any());
        verify(metadata, never()).getIndexInfo(any(), any(), any(), anyBoolean(), anyBoolean());
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldUseSqlUniqueIndexWhenMysqlTableHasNoPrimaryKey() throws SQLException {
        YashanDbConnection mysqlConnection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(true)) {
            @Override
            public Connection connection() {
                return YashanDbConnectionSchemaTest.this.jdbcConnection;
            }
        };
        PreparedStatement tablesStatement = mock(PreparedStatement.class);
        PreparedStatement columnsStatement = mock(PreparedStatement.class);
        PreparedStatement uniqueIndexStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(contains("information_schema.tables"))).thenReturn(tablesStatement);
        when(jdbcConnection.prepareStatement(contains("information_schema.columns"))).thenReturn(columnsStatement);
        when(jdbcConnection.prepareStatement(contains("information_schema.statistics"))).thenReturn(uniqueIndexStatement);
        doReturn(resultSet(new Object[]{ "APP", "ORDERS" })).when(tablesStatement).executeQuery();
        doReturn(resultSet(mysqlColumn("APP", "ORDERS", "ID", "bigint", null, 19, 0, "NO", null, 1, null),
                mysqlColumn("APP", "ORDERS", "REGION", "varchar", 32, null, null, "NO", null, 2, null)))
                .when(columnsStatement).executeQuery();
        doReturn(resultSet(new Object[]{ "APP", "ORDERS", "ORDERS_UK", "U", "REGION", 1 },
                new Object[]{ "APP", "ORDERS", "ORDERS_UK", "U", "ID", 2 }))
                .when(uniqueIndexStatement).executeQuery();

        mysqlConnection.readSchema(tables, null, "APP", null, null, false);

        TableId orders = new TableId(null, "APP", "ORDERS");
        assertThat(tables.forTable(orders).primaryKeyColumnNames()).containsExactly("REGION", "ID");
        verify(metadata, never()).getTables(any(), any(), any(), any());
        verify(metadata, never()).getPrimaryKeys(any(), any(), any());
        verify(metadata, never()).getIndexInfo(any(), any(), any(), anyBoolean(), anyBoolean());
        verify(uniqueIndexStatement).setString(1, "APP");
        verify(uniqueIndexStatement).setString(2, "ORDERS");
        verify(jdbcConnection).prepareStatement(contains("ORDER BY table_name"));
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldBatchMysqlColumnMetadataByTableCount() throws SQLException {
        int tableCount = 1001;
        YashanDbConnection mysqlConnection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(true)) {
            @Override
            public Connection connection() {
                return YashanDbConnectionSchemaTest.this.jdbcConnection;
            }
        };
        PreparedStatement tablesStatement = mock(PreparedStatement.class);
        PreparedStatement firstColumnsStatement = mock(PreparedStatement.class);
        PreparedStatement secondColumnsStatement = mock(PreparedStatement.class);
        PreparedStatement firstKeyStatement = mock(PreparedStatement.class);
        PreparedStatement secondKeyStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(contains("information_schema.tables"))).thenReturn(tablesStatement);
        when(jdbcConnection.prepareStatement(contains("information_schema.columns")))
                .thenReturn(firstColumnsStatement, secondColumnsStatement);
        when(jdbcConnection.prepareStatement(contains("non_unique = 0"))).thenReturn(firstKeyStatement, secondKeyStatement);

        Object[][] tableRows = new Object[tableCount][];
        Object[][] firstBatchColumns = new Object[1000][];
        Object[][] secondBatchColumns = new Object[1][];
        for (int i = 0; i < tableCount; i++) {
            String tableName = String.format("ORDERS_%04d", i);
            tableRows[i] = new Object[]{ "APP", tableName };
            Object[] columnRow = mysqlColumn("APP", tableName, "ID", "bigint", null, 19, 0, "NO", null, 1, null);
            if (i < firstBatchColumns.length) {
                firstBatchColumns[i] = columnRow;
            }
            else {
                secondBatchColumns[0] = columnRow;
            }
        }
        doReturn(resultSet(tableRows)).when(tablesStatement).executeQuery();
        doReturn(resultSet(firstBatchColumns)).when(firstColumnsStatement).executeQuery();
        doReturn(resultSet(secondBatchColumns)).when(secondColumnsStatement).executeQuery();
        doReturn(resultSet(new Object[]{ "APP", "ORDERS_0999", "PRIMARY", "P", "ID", 1 }))
                .when(firstKeyStatement).executeQuery();
        doReturn(resultSet(new Object[]{ "APP", "ORDERS_1000", "PRIMARY", "P", "ID", 1 }))
                .when(secondKeyStatement).executeQuery();

        mysqlConnection.readSchema(tables, null, "APP", null, null, false);

        assertThat(tables.tableIds()).hasSize(tableCount);
        assertThat(tables.forTable(new TableId(null, "APP", "ORDERS_0000")).columns()).extracting(Column::name).containsExactly("ID");
        assertThat(tables.forTable(new TableId(null, "APP", "ORDERS_1000")).columns()).extracting(Column::name).containsExactly("ID");
        assertThat(tables.forTable(new TableId(null, "APP", "ORDERS_0999")).primaryKeyColumnNames()).containsExactly("ID");
        assertThat(tables.forTable(new TableId(null, "APP", "ORDERS_1000")).primaryKeyColumnNames()).containsExactly("ID");
        verify(firstColumnsStatement).setString(1, "APP");
        verify(firstColumnsStatement).setString(2, "ORDERS_0000");
        verify(firstColumnsStatement).setString(1001, "ORDERS_0999");
        verify(firstColumnsStatement, never()).setString(eq(1002), anyString());
        verify(secondColumnsStatement).setString(1, "APP");
        verify(secondColumnsStatement).setString(2, "ORDERS_1000");
        verify(secondColumnsStatement, never()).setString(eq(3), anyString());
        verify(firstKeyStatement).setString(1, "APP");
        verify(firstKeyStatement).setString(1001, "ORDERS_0999");
        verify(firstKeyStatement, never()).setString(eq(1002), anyString());
        verify(secondKeyStatement).setString(1, "APP");
        verify(secondKeyStatement).setString(2, "ORDERS_1000");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldKeepMysqlTablesThatDifferOnlyByCaseSeparate() throws SQLException {
        YashanDbConnection mysqlConnection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(true)) {
            @Override
            public Connection connection() {
                return YashanDbConnectionSchemaTest.this.jdbcConnection;
            }
        };
        PreparedStatement tablesStatement = mock(PreparedStatement.class);
        PreparedStatement columnsStatement = mock(PreparedStatement.class);
        PreparedStatement keysStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(contains("information_schema.tables"))).thenReturn(tablesStatement);
        when(jdbcConnection.prepareStatement(contains("information_schema.columns"))).thenReturn(columnsStatement);
        when(jdbcConnection.prepareStatement(contains("non_unique = 0"))).thenReturn(keysStatement);
        doReturn(resultSet(new Object[]{ "APP", "Orders" }, new Object[]{ "APP", "orders" }))
                .when(tablesStatement).executeQuery();
        doReturn(resultSet(
                mysqlColumn("APP", "Orders", "UPPER_ID", "bigint", null, 19, 0, "NO", null, 1, null),
                mysqlColumn("APP", "orders", "LOWER_ID", "bigint", null, 19, 0, "NO", null, 1, null)))
                .when(columnsStatement).executeQuery();
        doReturn(resultSet()).when(keysStatement).executeQuery();

        mysqlConnection.readSchema(tables, null, "APP", null, null, false);

        TableId upperCase = new TableId(null, "APP", "Orders");
        TableId lowerCase = new TableId(null, "APP", "orders");
        assertThat(tables.tableIds()).containsExactlyInAnyOrder(upperCase, lowerCase);
        assertThat(tables.forTable(upperCase).columns()).extracting(Column::name).containsExactly("UPPER_ID");
        assertThat(tables.forTable(lowerCase).columns()).extracting(Column::name).containsExactly("LOWER_ID");
    }

    private static TableId tableId(String table) {
        return new TableId(null, "APP", table);
    }

    private static Object[] mysqlColumn(String schema, String table, String name, String typeName,
                                        Integer characterMaximumLength, Integer numericPrecision, Integer numericScale,
                                        String nullable, String defaultValue, int position, String extra) {
        return new Object[]{ schema, table, name, typeName, characterMaximumLength, numericPrecision, numericScale,
                nullable, defaultValue, position, extra };
    }

    private static ResultSet resultSet(Object[]... rows) throws SQLException {
        ResultSet result = mock(ResultSet.class);
        AtomicInteger index = new AtomicInteger(-1);
        when(result.next()).thenAnswer(invocation -> index.incrementAndGet() < rows.length);
        when(result.getString(anyInt())).thenAnswer(invocation -> {
            Object value = rows[index.get()][invocation.<Integer> getArgument(0) - 1];
            return value == null ? null : value.toString();
        });
        when(result.getInt(anyInt())).thenAnswer(invocation -> {
            Object value = rows[index.get()][invocation.<Integer> getArgument(0) - 1];
            return value == null ? 0 : ((Number) value).intValue();
        });
        when(result.getObject(anyInt())).thenAnswer(invocation -> rows[index.get()][invocation.<Integer> getArgument(0) - 1]);
        return result;
    }
}
