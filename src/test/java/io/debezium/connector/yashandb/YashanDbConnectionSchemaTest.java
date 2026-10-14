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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
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
    void shouldReadAllTablesUsingSchemaAndTableNames() throws SQLException {
        ResultSet tableMetadata = nativeTableMetadata("APP", "ORDERS", "APP", "CUSTOMERS");
        PreparedStatement tablesStatement = nativeTablesStatement(tableMetadata);
        ResultSet columnMetadata = resultSet(
                nativeColumn("APP", "CUSTOMERS", "ID", "INTEGER", 22, null, null, 0, "Y", null, 1),
                nativeColumn("APP", "ORDERS", "DESCRIPTION", "VARCHAR2", 100, 100, null, null, "Y", null, 2),
                nativeColumn("APP", "ORDERS", "ID", "NUMBER", 22, null, 10, null, "Y", "0", 1));
        PreparedStatement columnsStatement = nativeColumnsStatement(columnMetadata);
        PreparedStatement keysStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(contains("all_ind_columns"))).thenReturn(keysStatement);
        doReturn(resultSet(new Object[]{ "APP", "ORDERS", "ORDERS_PK", "P", "ID", 1 },
                new Object[]{ "APP", "ORDERS", "ORDERS_PK", "P", "DESCRIPTION", 2 }))
                .when(keysStatement).executeQuery();

        connection.readSchema(tables, null, "APP", null, null, false);

        assertThat(tables.tableIds()).containsExactlyInAnyOrder(tableId("ORDERS"), tableId("CUSTOMERS"));
        Table orders = tables.forTable(tableId("ORDERS"));
        assertThat(orders.columns()).extracting(Column::name).containsExactly("ID", "DESCRIPTION");
        assertThat(orders.primaryKeyColumnNames()).containsExactly("ID", "DESCRIPTION");
        assertThat(orders.columnWithName("ID").scale()).isEmpty();
        assertThat(orders.columnWithName("ID").defaultValueExpression()).contains("0");
        assertThat(orders.columnWithName("DESCRIPTION").isOptional()).isTrue();
        verify(metadata, never()).getColumns(any(), any(), any(), any());
        verify(tablesStatement).setString(1, "APP");
        verify(tableMetadata).close();
        verify(columnMetadata).close();
        verify(columnsStatement).setString(1, "APP");
        verify(columnsStatement).setString(2, "ORDERS");
        verify(columnsStatement).setString(3, "CUSTOMERS");
        verify(keysStatement).setString(1, "APP");
        verify(keysStatement).setString(2, "ORDERS");
        verify(keysStatement).setString(3, "CUSTOMERS");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldReadNativeKeysForTablesInOneBatchQuery() throws SQLException {
        nativeTablesStatement(nativeTableMetadata("APP", "ORDERS", "APP", "CUSTOMERS"));
        nativeColumnsStatement(resultSet(
                nativeColumn("APP", "ORDERS", "REGION", "VARCHAR2", 16, 16, null, null, "N", null, 1),
                nativeColumn("APP", "ORDERS", "ID", "INTEGER", 22, null, null, 0, "N", null, 1),
                nativeColumn("APP", "CUSTOMERS", "ID", "INTEGER", 22, null, null, 0, "N", null, 1)));
        PreparedStatement keysStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(contains("all_ind_columns"))).thenReturn(keysStatement);
        doReturn(resultSet(
                new Object[]{ "APP", "ORDERS", "ORDERS_PK", "P", "REGION", 1 },
                new Object[]{ "APP", "ORDERS", "ORDERS_PK", "P", "ID", 2 },
                new Object[]{ "APP", "CUSTOMERS", "CUSTOMERS_PK", "P", "ID", 1 }))
                .when(keysStatement).executeQuery();

        connection.readSchema(tables, null, "APP", null, null, false);

        assertThat(tables.forTable(tableId("ORDERS")).primaryKeyColumnNames()).containsExactly("REGION", "ID");
        assertThat(tables.forTable(tableId("CUSTOMERS")).primaryKeyColumnNames()).containsExactly("ID");
        verify(metadata, never()).getPrimaryKeys(any(), any(), any());
        verify(metadata, never()).getIndexInfo(any(), any(), any(), anyBoolean(), anyBoolean());
        verify(keysStatement).setString(1, "APP");
        verify(keysStatement).setString(2, "ORDERS");
        verify(keysStatement).setString(3, "CUSTOMERS");
        verify(jdbcConnection).prepareStatement(contains("all_ind_columns"));
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
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldApplyFiltersEvenWhenFullColumnScanIsForced(boolean forceFullColumnScan) throws SQLException {
        connection = new YashanDbConnection(JdbcConfiguration.create()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_FULL_COLUMN_SCAN_FORCE.name(), Boolean.toString(forceFullColumnScan))
                .build(), config -> jdbcConnection);
        nativeTablesStatement(nativeTableMetadata("APP", "ORDERS", "APP", "EXCLUDED"));
        PreparedStatement columnsStatement = nativeColumnsStatement(resultSet(
                nativeColumn("APP", "ORDERS", "ID", "INTEGER", 22, null, null, 0, "Y", null, 1),
                nativeColumn("APP", "ORDERS", "SECRET", "VARCHAR2", 100, 100, null, null, "Y", null, 2)));

        connection.readSchema(tables, null, "APP", Tables.TableFilter.fromPredicate(tableId("ORDERS")::equals),
                (catalog, schema, table, column) -> !column.equals("SECRET"), false);

        assertThat(tables.tableIds()).containsExactly(tableId("ORDERS"));
        assertThat(tables.forTable(tableId("ORDERS")).columns()).extracting(Column::name).containsExactly("ID");
        verify(metadata, never()).getColumns(any(), any(), any(), any());
        verify(columnsStatement).setString(2, "ORDERS");
        verify(columnsStatement, never()).setString(eq(3), anyString());
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldBindExactSchemaAndTableNamesForColumnMetadata() throws SQLException {
        PreparedStatement tablesStatement = nativeTablesStatement(nativeTableMetadata("APP_SCHEMA", "ORDER_%"));
        PreparedStatement columnsStatement = nativeColumnsStatement(
                resultSet(nativeColumn("APP_SCHEMA", "ORDER_%", "ID", "INTEGER", 22, null, null, 0, "Y", null, 1)));

        connection.readSchema(tables, null, "APP_SCHEMA", null, null, false);

        assertThat(tables.tableIds()).containsExactly(new TableId(null, "APP_SCHEMA", "ORDER_%"));
        verify(jdbcConnection).prepareStatement("SELECT owner, table_name FROM all_tables WHERE owner = ?");
        verify(tablesStatement).setString(1, "APP_SCHEMA");
        verify(columnsStatement).setString(1, "APP_SCHEMA");
        verify(columnsStatement).setString(2, "ORDER_%");
        verify(metadata, never()).getColumns(any(), any(), any(), any());
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldResolveNativeColumnTypesFromYashanDbDocumentation() throws SQLException {
        nativeTablesStatement(nativeTableMetadata("APP", "TYPE_SAMPLES"));
        PreparedStatement columnsStatement = nativeColumnsStatement(resultSet(
                nativeColumn("APP", "TYPE_SAMPLES", "C_CHAR", "CHAR(10)", 10, 10, null, null, "Y", null, 1),
                nativeColumn("APP", "TYPE_SAMPLES", "C_CHARACTER", "CHARACTER(20)", 20, 20, null, null, "Y", null, 2),
                nativeColumn("APP", "TYPE_SAMPLES", "C_VARCHAR2", "VARCHAR2(100)", 100, 100, null, null, "Y", null, 3),
                nativeColumn("APP", "TYPE_SAMPLES", "C_CHARACTER_VARYING", "CHARACTER VARYING(120)", 120, 120, null, null, "Y", null, 4),
                nativeColumn("APP", "TYPE_SAMPLES", "C_NCHAR", "NCHAR(5)", 10, 5, null, null, "Y", null, 5),
                nativeColumn("APP", "TYPE_SAMPLES", "C_NVARCHAR2", "NVARCHAR2(50)", 100, 50, null, null, "Y", null, 6),
                nativeColumn("APP", "TYPE_SAMPLES", "C_NUMBER", "NUMBER(10,2)", 22, null, 10, 2, "Y", null, 7),
                nativeColumn("APP", "TYPE_SAMPLES", "C_NUMBER_FLOAT", "NUMBER", 22, null, null, null, "Y", null, 8),
                nativeColumn("APP", "TYPE_SAMPLES", "C_TINYINT", "TINYINT", 1, null, null, null, "Y", null, 9),
                nativeColumn("APP", "TYPE_SAMPLES", "C_SMALLINT", "SMALLINT", 2, null, null, null, "Y", null, 10),
                nativeColumn("APP", "TYPE_SAMPLES", "C_INTEGER", "INTEGER", 4, null, null, null, "Y", null, 11),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BIGINT", "BIGINT", 8, null, null, null, "Y", null, 12),
                nativeColumn("APP", "TYPE_SAMPLES", "C_FLOAT", "FLOAT", 4, null, 126, null, "Y", null, 13),
                nativeColumn("APP", "TYPE_SAMPLES", "C_REAL", "REAL", 4, null, 126, null, "Y", null, 14),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BINARY_FLOAT", "BINARY_FLOAT", 4, null, 126, null, "Y", null, 15),
                nativeColumn("APP", "TYPE_SAMPLES", "C_DOUBLE", "DOUBLE", 8, null, 53, null, "Y", null, 16),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BINARY_DOUBLE", "BINARY_DOUBLE", 8, null, 53, null, "Y", null, 17),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BOOLEAN", "BOOLEAN", 1, null, null, null, "Y", null, 18),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BIT", "BIT(8)", 8, null, null, null, "Y", null, 19),
                nativeColumn("APP", "TYPE_SAMPLES", "C_PLS_INTEGER", "PLS_INTEGER", 4, null, null, null, "Y", null, 20),
                nativeColumn("APP", "TYPE_SAMPLES", "C_DATE", "DATE", 8, null, null, null, "Y", null, 21),
                nativeColumn("APP", "TYPE_SAMPLES", "C_TIME", "TIME(6)", 8, null, null, 6, "Y", null, 22),
                nativeColumn("APP", "TYPE_SAMPLES", "C_TIMESTAMP", "TIMESTAMP(3)", 8, null, null, 3, "Y", null, 23),
                nativeColumn("APP", "TYPE_SAMPLES", "C_TIMESTAMP_TZ", "TIMESTAMP(6) WITH TIME ZONE", 8, null, null, 6, "Y", null, 24),
                nativeColumn("APP", "TYPE_SAMPLES", "C_TIMESTAMP_LTZ", "TIMESTAMP(6) WITH LOCAL TIME ZONE", 8, null, null, 6, "Y", null, 25),
                nativeColumn("APP", "TYPE_SAMPLES", "C_INTERVAL_YM", "INTERVAL YEAR(2) TO MONTH", 4, null, 2, null, "Y", null, 26),
                nativeColumn("APP", "TYPE_SAMPLES", "C_INTERVAL_DS", "INTERVAL DAY(2) TO SECOND(6)", 8, null, 2, 6, "Y", null, 27),
                nativeColumn("APP", "TYPE_SAMPLES", "C_JSON", "JSON", 33554432, null, null, null, "Y", null, 28),
                nativeColumn("APP", "TYPE_SAMPLES", "C_CLOB", "CLOB", 0, null, null, null, "Y", null, 29),
                nativeColumn("APP", "TYPE_SAMPLES", "C_NCLOB", "NCLOB", 0, null, null, null, "Y", null, 30),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BLOB", "BLOB", 0, null, null, null, "Y", null, 31),
                nativeColumn("APP", "TYPE_SAMPLES", "C_RAW", "RAW(16)", 16, null, null, null, "Y", null, 32),
                nativeColumn("APP", "TYPE_SAMPLES", "C_UROWID", "UROWID", 4000, null, null, null, "Y", null, 33),
                nativeColumn("APP", "TYPE_SAMPLES", "C_ROWID", "ROWID", 16, null, null, null, "Y", null, 34),
                nativeColumn("APP", "TYPE_SAMPLES", "C_XMLTYPE", "XMLTYPE", 0, null, null, null, "Y", null, 35),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BFILE", "BFILE", 0, null, null, null, "Y", null, 36),
                nativeColumn("APP", "TYPE_SAMPLES", "C_GEOMETRY", "ST_GEOMETRY", 0, null, null, null, "Y", null, 37),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BOX2D", "BOX2D", 32, null, null, null, "Y", null, 38),
                nativeColumn("APP", "TYPE_SAMPLES", "C_VECTOR", "VECTOR(4, FLOAT64)", 0, null, null, null, "Y", null, 39),
                nativeColumn("APP", "TYPE_SAMPLES", "C_OBJECT", "OBJECT", 0, null, null, null, "Y", null, 40),
                nativeColumn("APP", "TYPE_SAMPLES", "C_ARRAY", "ARRAY", 0, null, null, null, "Y", null, 41),
                nativeColumn("APP", "TYPE_SAMPLES", "C_REF", "REF", 0, null, null, null, "Y", null, 42)));

        connection.readSchema(tables, null, "APP", null, null, false);

        Table table = tables.forTable(tableId("TYPE_SAMPLES"));
        assertColumn(table, "C_CHAR", Types.CHAR, 10);
        assertColumn(table, "C_CHARACTER", Types.CHAR, 20);
        assertColumn(table, "C_VARCHAR2", Types.VARCHAR, 100);
        assertColumn(table, "C_CHARACTER_VARYING", Types.VARCHAR, 120);
        assertColumn(table, "C_NCHAR", Types.NCHAR, 5);
        assertColumn(table, "C_NVARCHAR2", Types.NVARCHAR, 50);
        assertColumn(table, "C_NUMBER", Types.NUMERIC, 10);
        assertColumn(table, "C_NUMBER_FLOAT", Types.NUMERIC, 38);
        assertColumn(table, "C_TINYINT", Types.TINYINT, 1);
        assertColumn(table, "C_SMALLINT", Types.SMALLINT, 2);
        assertColumn(table, "C_INTEGER", Types.INTEGER, 4);
        assertColumn(table, "C_BIGINT", Types.BIGINT, 8);
        assertColumn(table, "C_FLOAT", Types.REAL, 126);
        assertColumn(table, "C_REAL", Types.REAL, 126);
        assertColumn(table, "C_BINARY_FLOAT", Types.REAL, 126);
        assertColumn(table, "C_DOUBLE", Types.DOUBLE, 53);
        assertColumn(table, "C_BINARY_DOUBLE", Types.DOUBLE, 53);
        assertColumn(table, "C_BOOLEAN", Types.BOOLEAN, 1);
        assertColumn(table, "C_BIT", Types.BIT, 8);
        assertColumn(table, "C_PLS_INTEGER", Types.INTEGER, 4);
        assertColumn(table, "C_DATE", Types.TIMESTAMP, 0);
        assertColumn(table, "C_TIME", Types.TIME, 6);
        assertColumn(table, "C_TIMESTAMP", Types.TIMESTAMP, 3);
        assertColumn(table, "C_TIMESTAMP_TZ", com.yashandb.jdbc.YasTypes.TIMESTAMP_TZ, 6);
        assertColumn(table, "C_TIMESTAMP_LTZ", Types.TIMESTAMP, 6);
        assertColumn(table, "C_INTERVAL_YM", com.yashandb.jdbc.YasTypes.YM_INTERVAL, 2);
        assertColumn(table, "C_INTERVAL_DS", com.yashandb.jdbc.YasTypes.DS_INTERVAL, 2);
        assertColumn(table, "C_JSON", com.yashandb.jdbc.YasTypes.JSON, 33554432);
        assertColumn(table, "C_CLOB", Types.CLOB, 0);
        assertColumn(table, "C_NCLOB", Types.NCLOB, 0);
        assertColumn(table, "C_BLOB", Types.BLOB, 0);
        assertColumn(table, "C_RAW", com.yashandb.jdbc.YasTypes.RAW, 16);
        assertColumn(table, "C_UROWID", com.yashandb.jdbc.YasTypes.RAW, 4000);
        assertColumn(table, "C_ROWID", com.yashandb.jdbc.YasTypes.ROWID, 16);
        assertColumn(table, "C_XMLTYPE", com.yashandb.jdbc.YasTypes.SQLXML, 0);
        assertColumn(table, "C_BFILE", Types.BLOB, 0);
        assertColumn(table, "C_GEOMETRY", Types.BLOB, 0);
        assertColumn(table, "C_BOX2D", Types.BLOB, 32);
        assertColumn(table, "C_VECTOR", Types.OTHER, 0);
        assertColumn(table, "C_OBJECT", Types.STRUCT, 0);
        assertColumn(table, "C_ARRAY", Types.ARRAY, 0);
        assertColumn(table, "C_REF", Types.REF, 0);
        assertThat(table.columnWithName("C_NUMBER").scale()).contains(2);
        assertThat(table.columnWithName("C_NUMBER_FLOAT").scale()).isEmpty();
        assertThat(table.columnWithName("C_TIMESTAMP").scale()).contains(3);
        verify(columnsStatement).setString(2, "TYPE_SAMPLES");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldNotResolveDataTypesThatAreNotSupportedInYashanDbNativeMode() throws SQLException {
        nativeTablesStatement(nativeTableMetadata("APP", "TYPE_SAMPLES"));
        PreparedStatement columnsStatement = nativeColumnsStatement(resultSet(
                // mysql-mode data types
                nativeColumn("APP", "TYPE_SAMPLES", "C_TINYINT_UNSIGNED", "TINYINT UNSIGNED", 1, null, null, null, "Y", null, 1),
                nativeColumn("APP", "TYPE_SAMPLES", "C_SMALLINT_UNSIGNED", "SMALLINT UNSIGNED", 2, null, null, null, "Y", null, 2),
                nativeColumn("APP", "TYPE_SAMPLES", "C_INT_UNSIGNED", "INT UNSIGNED", 4, null, null, null, "Y", null, 3),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BIGINT_UNSIGNED", "BIGINT UNSIGNED", 8, null, null, null, "Y", null, 4),
                nativeColumn("APP", "TYPE_SAMPLES", "C_UTINYINT", "UTINYINT", 1, null, null, null, "Y", null, 5),
                nativeColumn("APP", "TYPE_SAMPLES", "C_USMALLINT", "USMALLINT", 2, null, null, null, "Y", null, 6),
                nativeColumn("APP", "TYPE_SAMPLES", "C_UINTEGER", "UINTEGER", 4, null, null, null, "Y", null, 7),
                nativeColumn("APP", "TYPE_SAMPLES", "C_UBIGINT", "UBIGINT", 8, null, null, null, "Y", null, 8),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BOOL", "BOOL", 1, null, null, null, "Y", null, 9),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BINARY", "BINARY", 255, null, null, null, "Y", null, 10),
                nativeColumn("APP", "TYPE_SAMPLES", "C_VARBINARY", "VARBINARY", 65534, null, null, null, "Y", null, 11),
                nativeColumn("APP", "TYPE_SAMPLES", "C_LONGVARBINARY", "LONGVARBINARY", 0, null, null, null, "Y", null, 12),
                // internal JDBC driver type names
                nativeColumn("APP", "TYPE_SAMPLES", "C_SHORTDATE", "SHORTDATE", 8, null, null, null, "Y", null, 13),
                nativeColumn("APP", "TYPE_SAMPLES", "C_SHORTTIME", "SHORTTIME", 8, null, null, 6, "Y", null, 14),
                nativeColumn("APP", "TYPE_SAMPLES", "C_TIMESTAMP_TZ", "TIMESTAMP_TZ", 8, null, null, 6, "Y", null, 15),
                nativeColumn("APP", "TYPE_SAMPLES", "C_TIMESTAMP_LTZ", "TIMESTAMP_LTZ", 8, null, null, 6, "Y", null, 16),
                nativeColumn("APP", "TYPE_SAMPLES", "C_YM_INTERVAL", "YM_INTERVAL", 4, null, 2, null, "Y", null, 17),
                nativeColumn("APP", "TYPE_SAMPLES", "C_DS_INTERVAL", "DS_INTERVAL", 8, null, 2, 6, "Y", null, 18),
                nativeColumn("APP", "TYPE_SAMPLES", "C_CURSOR", "CURSOR", 0, null, null, null, "Y", null, 19),
                // legacy JDBC type names and undocumented aliases
                nativeColumn("APP", "TYPE_SAMPLES", "C_LONGVARCHAR", "LONGVARCHAR", 0, null, null, null, "Y", null, 20),
                nativeColumn("APP", "TYPE_SAMPLES", "C_LONGNVARCHAR", "LONGNVARCHAR", 0, null, null, null, "Y", null, 21),
                nativeColumn("APP", "TYPE_SAMPLES", "C_XML", "XML", 0, null, null, null, "Y", null, 22),
                nativeColumn("APP", "TYPE_SAMPLES", "C_STRUCT", "STRUCT", 0, null, null, null, "Y", null, 23),
                nativeColumn("APP", "TYPE_SAMPLES", "C_TIMESTAMP_WITH_TIMEZONE", "TIMESTAMP WITH TIMEZONE", 8, null, null, 6, "Y", null, 24),
                // subtypes of ST_GEOMETRY, which are not column data types of their own
                nativeColumn("APP", "TYPE_SAMPLES", "C_POINT", "POINT", 0, null, null, null, "Y", null, 25),
                nativeColumn("APP", "TYPE_SAMPLES", "C_GEOMETRYCOLLECTION", "GEOMETRYCOLLECTION", 0, null, null, null, "Y", null, 26),
                // native storage types that the numeric data type chapter does not list as column data types
                nativeColumn("APP", "TYPE_SAMPLES", "C_BINARY_TINYINT", "BINARY_TINYINT", 1, null, null, null, "Y", null, 27),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BINARY_SMALLINT", "BINARY_SMALLINT", 2, null, null, null, "Y", null, 28),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BINARY_INTEGER", "BINARY_INTEGER", 4, null, null, null, "Y", null, 29),
                nativeColumn("APP", "TYPE_SAMPLES", "C_BINARY_BIGINT", "BINARY_BIGINT", 8, null, null, null, "Y", null, 30)));

        connection.readSchema(tables, null, "APP", null, null, false);

        Table table = tables.forTable(tableId("TYPE_SAMPLES"));
        for (Column column : table.columns()) {
            assertThat(column.jdbcType()).as(column.name() + " JDBC type").isEqualTo(Types.OTHER);
        }
        assertColumn(table, "C_TINYINT_UNSIGNED", Types.OTHER, 1);
        assertColumn(table, "C_BIGINT_UNSIGNED", Types.OTHER, 8);
        assertColumn(table, "C_VARBINARY", Types.OTHER, 65534);
        assertColumn(table, "C_TIMESTAMP_TZ", Types.OTHER, 8);
        assertColumn(table, "C_SHORTDATE", Types.OTHER, 8);
        assertColumn(table, "C_BINARY_TINYINT", Types.OTHER, 1);
        assertColumn(table, "C_BINARY_BIGINT", Types.OTHER, 8);
        verify(columnsStatement).setString(2, "TYPE_SAMPLES");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldReadColumnsFromEachSchemaWhenSchemaIsNotSpecified() throws SQLException {
        nativeTablesStatement(nativeTableMetadata("APP", "ORDERS", "OTHER", "ORDERS"));
        PreparedStatement columnsStatement = nativeColumnsStatement(resultSet());
        doReturn(resultSet(nativeColumn("APP", "ORDERS", "ID", "INTEGER", 22, null, null, 0, "Y", null, 1)),
                resultSet(nativeColumn("OTHER", "ORDERS", "ID", "INTEGER", 22, null, null, 0, "Y", null, 1)))
                .when(columnsStatement).executeQuery();

        connection.readSchema(tables, null, null, null, null, false);

        assertThat(tables.tableIds()).containsExactlyInAnyOrder(tableId("ORDERS"), new TableId(null, "OTHER", "ORDERS"));
        verify(jdbcConnection).prepareStatement("SELECT owner, table_name FROM all_tables");
        verify(columnsStatement).setString(1, "APP");
        verify(columnsStatement).setString(1, "OTHER");
        verify(columnsStatement, times(2)).setString(2, "ORDERS");
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
    void shouldUseUniqueIndexWhenTableHasNoPrimaryKey() throws SQLException {
        nativeTablesStatement(nativeTableMetadata("APP", "ORDERS"));
        nativeColumnsStatement(resultSet(nativeColumn("APP", "ORDERS", "ID", "INTEGER", 22, null, null, 0, "Y", null, 1)));
        PreparedStatement keysStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(contains("all_ind_columns"))).thenReturn(keysStatement);
        doReturn(resultSet(new Object[]{ "APP", "ORDERS", "ORDERS_UK", "U", "ID", 1 })).when(keysStatement).executeQuery();

        connection.readSchema(tables, null, "APP", null, null, false);

        assertThat(tables.forTable(tableId("ORDERS")).primaryKeyColumnNames()).containsExactly("ID");
    }

    @FixFor("debezium/dbz#3014")
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldHonorRemovalFlagWhenNoTablesAreFound(boolean removeTablesNotFoundInJdbc) throws SQLException {
        tables.overwriteTable(tableId("DROPPED"), List.of(Column.editor().name("ID").position(1).create()), List.of("ID"), null, List.of());
        nativeTablesStatement(resultSet());

        connection.readSchema(tables, null, "APP", null, null, removeTablesNotFoundInJdbc);

        assertThat(tables.forTable(tableId("DROPPED")) == null).isEqualTo(removeTablesNotFoundInJdbc);
        verify(metadata, never()).getColumns(any(), any(), any(), any());
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldRetainTableDefinitionWhenAllColumnsAreExcluded() throws SQLException {
        nativeTablesStatement(nativeTableMetadata("APP", "ORDERS"));
        nativeColumnsStatement(resultSet(nativeColumn("APP", "ORDERS", "ID", "INTEGER", 22, null, null, 0, "Y", null, 1)));

        connection.readSchema(tables, null, "APP", null, (catalog, schema, table, column) -> false, true);

        assertThat(tables.tableIds()).containsExactly(tableId("ORDERS"));
        assertThat(tables.forTable(tableId("ORDERS")).columns()).isEmpty();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldRetainOnlyTablesWithIncludedColumnsWhenSomeTablesAreFullyExcluded() throws SQLException {
        nativeTablesStatement(nativeTableMetadata("APP", "ORDERS", "APP", "AUDIT"));
        nativeColumnsStatement(resultSet(
                nativeColumn("APP", "AUDIT", "SECRET", "VARCHAR2", 100, 100, null, null, "Y", null, 1),
                nativeColumn("APP", "ORDERS", "ID", "INTEGER", 22, null, null, 0, "Y", null, 1)));

        connection.readSchema(tables, null, "APP", null, (catalog, schema, table, column) -> !column.equals("SECRET"), false);

        assertThat(tables.tableIds()).containsExactly(tableId("ORDERS"));
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
    void shouldBatchNativeColumnMetadataByTableCount() throws SQLException {
        int tableCount = 1001;
        PreparedStatement firstColumnsStatement = mock(PreparedStatement.class);
        PreparedStatement secondColumnsStatement = mock(PreparedStatement.class);
        PreparedStatement firstKeyStatement = mock(PreparedStatement.class);
        PreparedStatement secondKeyStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(contains("all_tab_cols")))
                .thenReturn(firstColumnsStatement, secondColumnsStatement);
        when(jdbcConnection.prepareStatement(contains("all_ind_columns"))).thenReturn(firstKeyStatement, secondKeyStatement);

        Object[][] tableRows = new Object[tableCount][];
        Object[][] firstBatchColumns = new Object[1000][];
        Object[][] secondBatchColumns = new Object[1][];
        for (int i = 0; i < tableCount; i++) {
            String tableName = String.format("ORDERS_%04d", i);
            tableRows[i] = new Object[]{ "APP", tableName };
            Object[] columnRow = nativeColumn("APP", tableName, "ID", "INTEGER", 22, null, null, 0, "Y", null, 1);
            if (i < firstBatchColumns.length) {
                firstBatchColumns[i] = columnRow;
            }
            else {
                secondBatchColumns[0] = columnRow;
            }
        }
        nativeTablesStatement(resultSet(tableRows));
        doReturn(resultSet(firstBatchColumns)).when(firstColumnsStatement).executeQuery();
        doReturn(resultSet(secondBatchColumns)).when(secondColumnsStatement).executeQuery();
        doReturn(resultSet(new Object[]{ "APP", "ORDERS_0999", "ORDERS_PK", "P", "ID", 1 }))
                .when(firstKeyStatement).executeQuery();
        doReturn(resultSet(new Object[]{ "APP", "ORDERS_1000", "ORDERS_PK", "P", "ID", 1 }))
                .when(secondKeyStatement).executeQuery();

        connection.readSchema(tables, null, "APP", null, null, false);

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
        verify(firstKeyStatement).setString(1002, "APP");
        verify(firstKeyStatement).setString(2002, "ORDERS_0999");
        verify(firstKeyStatement, never()).setString(eq(2003), anyString());
        verify(secondKeyStatement).setString(1, "APP");
        verify(secondKeyStatement).setString(3, "APP");
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

    private static void assertColumn(Table table, String columnName, int jdbcType, int length) {
        Column column = table.columnWithName(columnName);
        assertThat(column.jdbcType()).as(columnName + " JDBC type").isEqualTo(jdbcType);
        assertThat(column.length()).as(columnName + " length").isEqualTo(length);
    }

    private PreparedStatement nativeTablesStatement(ResultSet rows) throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(contains("all_tables"))).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rows);
        return statement;
    }

    private PreparedStatement nativeColumnsStatement(ResultSet rows) throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(contains("all_tab_cols"))).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rows);
        return statement;
    }

    private static ResultSet nativeTableMetadata(String... ownerTableNames) throws SQLException {
        Object[][] rows = new Object[ownerTableNames.length / 2][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new Object[]{ ownerTableNames[i * 2], ownerTableNames[i * 2 + 1] };
        }
        return resultSet(rows);
    }

    private static Object[] nativeColumn(String schema, String table, String name, String dataType,
                                         Integer dataLength, Integer charLength, Integer precision, Integer scale,
                                         String nullable, String defaultValue, int position) {
        return new Object[]{ schema, table, name, dataType, dataLength, charLength, precision, scale, nullable,
                defaultValue, position };
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
