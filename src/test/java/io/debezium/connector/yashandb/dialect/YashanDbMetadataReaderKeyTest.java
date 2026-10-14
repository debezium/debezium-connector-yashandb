/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.connector.yashandb.YashanDbConnection;
import io.debezium.connector.yashandb.YashanDbModeContext;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.TableId;

class YashanDbMetadataReaderKeyTest {

    private final TableId table = new TableId(null, "APP", "ORDERS");
    private final YashanDbMetadataReader metadataReader = new YashanDbMySqlMetadataReader();
    private YashanDbConnection connection;
    private PreparedStatement statement;

    @BeforeEach
    void setUp() throws SQLException {
        Connection jdbcConnection = mock(Connection.class);
        statement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(anyString())).thenReturn(statement);
        connection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(true)) {
            @Override
            public Connection connection() {
                return jdbcConnection;
            }
        };
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldSkipEntireFilteredIndexAndReturnFirstIncludedCompositeIndex() throws SQLException {
        ResultSet rows = rows(new Object[]{ "A_SKIP", "FIRST", 1 }, new Object[]{ "A_SKIP", "SECRET", 2 },
                new Object[]{ "B_ACCEPT", "REGION", 1 }, new Object[]{ "B_ACCEPT", "ID", 2 },
                new Object[]{ "C_OTHER", "OTHER", 1 });
        when(statement.executeQuery()).thenReturn(rows);

        List<String> columns = metadataReader.readTableUniqueIndices(connection, table,
                (index, column) -> !"SECRET".equals(column), () -> {
                    throw new AssertionError("MySQL metadata must use SQL");
                });

        assertThat(columns).containsExactly("REGION", "ID");
        verify(statement).setString(1, "APP");
        verify(statement).setString(2, "ORDERS");
        verify(rows).close();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldReturnNoColumnsWhenAllUniqueIndicesAreFiltered() throws SQLException {
        ResultSet rows = rows(new Object[]{ "A_SKIP", "ID", 1 }, new Object[]{ "B_SKIP", "REGION", 1 });
        when(statement.executeQuery()).thenReturn(rows);

        assertThat(metadataReader.readTableUniqueIndices(connection, table, (index, column) -> false, () -> {
            throw new AssertionError("MySQL metadata must use SQL");
        })).isEmpty();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldReadCompositePrimaryKeyInQueryOrder() throws SQLException {
        ResultSet rows = rows(new Object[]{ "REGION" }, new Object[]{ "ID" });
        when(statement.executeQuery()).thenReturn(rows);

        assertThat(metadataReader.readPrimaryKeyNames(connection, table, () -> {
            throw new AssertionError("MySQL metadata must use SQL");
        })).containsExactly("REGION", "ID");
        verify(statement).setString(1, "APP");
        verify(statement).setString(2, "ORDERS");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldUseJdbcReadersForNativeKeys() throws SQLException {
        YashanDbMetadataReader nativeReader = new YashanDbNativeMetadataReader();

        assertThat(nativeReader.readPrimaryKeyNames(connection, table, () -> List.of("REGION", "ID")))
                .containsExactly("REGION", "ID");
        assertThat(nativeReader.readTableUniqueIndices(connection, table, (index, column) -> true, () -> List.of("ID")))
                .containsExactly("ID");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldReadMysqlKeysForAllTablesInSchemaBatch() throws SQLException {
        ResultSet rows = rows(new Object[]{ "APP", "ORDERS", "ORDERS_UK", "U", "EMAIL", 1 },
                new Object[]{ "APP", "ORDERS", "PRIMARY", "P", "REGION", 1 },
                new Object[]{ "APP", "ORDERS", "PRIMARY", "P", "ID", 2 },
                new Object[]{ "APP", "CUSTOMERS", "A_FILTERED", "U", "ID", 1 },
                new Object[]{ "APP", "CUSTOMERS", "A_FILTERED", "U", "SECRET", 2 },
                new Object[]{ "APP", "CUSTOMERS", "B_INCLUDED", "U", "EMAIL", 1 });
        when(statement.executeQuery()).thenReturn(rows);
        List<TableId> tables = List.of(table, new TableId(null, "APP", "CUSTOMERS"));

        Map<TableId, List<String>> keys = metadataReader.readPrimaryKeyOrUniqueIndexNames(connection, tables,
                (index, column) -> !"SECRET".equals(column));

        assertThat(keys.get(table)).containsExactly("REGION", "ID");
        assertThat(keys.get(tables.get(1))).containsExactly("EMAIL");
        verify(statement).setString(1, "APP");
        verify(statement).setString(2, "ORDERS");
        verify(statement).setString(3, "CUSTOMERS");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldIgnoreRowsFromOtherSchemas() throws SQLException {
        ResultSet rows = rows(new Object[]{ "OTHER", "ORDERS", "FOREIGN_UK", "U", "FOREIGN_ID", 1 },
                new Object[]{ "APP", "ORDERS", "ORDERS_UK", "U", "REGION", 1 });
        when(statement.executeQuery()).thenReturn(rows);

        Map<TableId, List<String>> keys = metadataReader.readPrimaryKeyOrUniqueIndexNames(connection, List.of(table),
                (index, column) -> true);

        assertThat(keys.get(table)).containsExactly("REGION");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldNotMergePrimaryConstraintAndBackingUniqueIndex() throws SQLException {
        ResultSet rows = rows(new Object[]{ "APP", "ORDERS", "ORDERS_PK", "P", "ID", 1 },
                new Object[]{ "APP", "ORDERS", "ORDERS_PK", "U", "ID", 1 });
        when(statement.executeQuery()).thenReturn(rows);

        Map<TableId, List<String>> keys = new YashanDbNativeMetadataReader().readPrimaryKeyOrUniqueIndexNames(connection,
                List.of(table), (index, column) -> true);

        assertThat(keys.get(table)).containsExactly("ID");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldKeepSameNamedTablesInSeparateSchemaQueries() throws SQLException {
        TableId otherSchemaTable = new TableId(null, "OTHER", "ORDERS");
        ResultSet appRows = rows(new Object[]{ "APP", "ORDERS", "PRIMARY", "P", "ID", 1 });
        ResultSet otherRows = rows(new Object[]{ "OTHER", "ORDERS", "PRIMARY", "P", "REGION", 1 });
        when(statement.executeQuery()).thenReturn(appRows, otherRows);

        Map<TableId, List<String>> keys = metadataReader.readPrimaryKeyOrUniqueIndexNames(connection,
                List.of(table, otherSchemaTable), (index, column) -> true);

        assertThat(keys.get(table)).containsExactly("ID");
        assertThat(keys.get(otherSchemaTable)).containsExactly("REGION");
        verify(statement).setString(1, "APP");
        verify(statement).setString(1, "OTHER");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldReadNativeKeysForAllTablesInSchemaBatch() throws SQLException {
        YashanDbMetadataReader nativeReader = new YashanDbNativeMetadataReader();
        ResultSet rows = rows(new Object[]{ "APP", "ORDERS", "ORDERS_PK", "P", "REGION", 1 },
                new Object[]{ "APP", "ORDERS", "ORDERS_PK", "P", "ID", 2 },
                new Object[]{ "APP", "CUSTOMERS", "CUSTOMERS_UK", "U", "EMAIL", 1 });
        when(statement.executeQuery()).thenReturn(rows);

        Map<TableId, List<String>> keys = nativeReader.readPrimaryKeyOrUniqueIndexNames(connection,
                List.of(table, new TableId(null, "APP", "CUSTOMERS")), (index, column) -> true);

        assertThat(keys.get(table)).containsExactly("REGION", "ID");
        assertThat(keys.get(new TableId(null, "APP", "CUSTOMERS"))).containsExactly("EMAIL");
        verify(statement).setString(1, "APP");
        verify(statement).setString(2, "ORDERS");
        verify(statement).setString(3, "CUSTOMERS");
        verify(statement).setString(4, "APP");
        verify(statement).setString(5, "ORDERS");
        verify(statement).setString(6, "CUSTOMERS");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldUseYashanCatalogKeyTypeValues() throws SQLException {
        ResultSet emptyRows = rows();
        when(statement.executeQuery()).thenReturn(emptyRows);

        new YashanDbNativeMetadataReader().readPrimaryKeyOrUniqueIndexNames(connection, List.of(table),
                (index, column) -> true);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(connection.connection()).prepareStatement(sql.capture());
        assertThat(sql.getValue()).contains("constraint_type IN ('PRIMARY KEY', 'P')", "uniqueness = 'Y'");
    }

    private static ResultSet rows(Object[]... values) throws SQLException {
        ResultSet result = mock(ResultSet.class);
        AtomicInteger rowIndex = new AtomicInteger(-1);
        when(result.next()).thenAnswer(invocation -> rowIndex.incrementAndGet() < values.length);
        when(result.getString(anyInt())).thenAnswer(invocation -> values[rowIndex.get()][invocation.<Integer> getArgument(0) - 1]);
        when(result.getInt(anyInt())).thenAnswer(invocation -> values[rowIndex.get()][invocation.<Integer> getArgument(0) - 1]);
        return result;
    }
}
