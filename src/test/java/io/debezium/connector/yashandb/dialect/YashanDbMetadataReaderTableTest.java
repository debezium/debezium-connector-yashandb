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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.debezium.connector.yashandb.YashanDbConnection;
import io.debezium.connector.yashandb.YashanDbModeContext;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.TableId;

class YashanDbMetadataReaderTableTest {

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldSelectIndependentMetadataReaderForEachMode() {
        assertThat(new YashanDbModeContext(false).metadataReader()).isInstanceOf(YashanDbNativeMetadataReader.class);
        assertThat(new YashanDbModeContext(true).metadataReader()).isInstanceOf(YashanDbMySqlMetadataReader.class);
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldReadNativeTableIdentifiersUsingSqlMetadata() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);
        Connection jdbcConnection = mock(Connection.class);
        when(jdbcConnection.prepareStatement(anyString())).thenReturn(statement);
        ResultSet rows = rows(new String[]{ "APP_SCHEMA", "ORDERS" }, new String[]{ "APP_SCHEMA", "CUSTOMERS" });
        when(statement.executeQuery()).thenReturn(rows);
        YashanDbConnection connection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(false)) {
            @Override
            public Connection connection() {
                return jdbcConnection;
            }
        };

        var tables = new YashanDbNativeMetadataReader().readTables(connection, null, "APP_SCHEMA");

        assertThat(tables).containsExactly(
                new YashanDbMetadataReader.TableMetadata(new TableId(null, "APP_SCHEMA", "ORDERS"), "TABLE"),
                new YashanDbMetadataReader.TableMetadata(new TableId(null, "APP_SCHEMA", "CUSTOMERS"), "TABLE"));
        verify(statement).setString(1, "APP_SCHEMA");
        verify(rows).close();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldReadNativeTablesWithoutSchemaFilterWhenSchemaIsNotSpecified() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);
        Connection jdbcConnection = mock(Connection.class);
        when(jdbcConnection.prepareStatement(anyString())).thenReturn(statement);
        ResultSet rows = rows(new String[]{ "APP_SCHEMA", "ORDERS" }, new String[]{ "OTHER_SCHEMA", "CUSTOMERS" });
        when(statement.executeQuery()).thenReturn(rows);
        YashanDbConnection connection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(false)) {
            @Override
            public Connection connection() {
                return jdbcConnection;
            }
        };

        var tables = new YashanDbNativeMetadataReader().readTables(connection, null, null);

        assertThat(tables).containsExactly(
                new YashanDbMetadataReader.TableMetadata(new TableId(null, "APP_SCHEMA", "ORDERS"), "TABLE"),
                new YashanDbMetadataReader.TableMetadata(new TableId(null, "OTHER_SCHEMA", "CUSTOMERS"), "TABLE"));
        verify(statement, never()).setString(anyInt(), anyString());
        verify(rows).close();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldUseCatalogAsMysqlSchemaWhenSchemaIsNotSupplied() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);
        Connection jdbcConnection = mock(Connection.class);
        when(jdbcConnection.prepareStatement(anyString())).thenReturn(statement);
        ResultSet rows = rows(new String[]{ null, "ORDERS" }, new String[]{ "", "CUSTOMERS" });
        when(statement.executeQuery()).thenReturn(rows);
        YashanDbConnection connection = new YashanDbConnection(JdbcConfiguration.create().build(), new YashanDbModeContext(true)) {
            @Override
            public Connection connection() {
                return jdbcConnection;
            }
        };

        var tables = new YashanDbMySqlMetadataReader().readTables(connection, "APP", null);

        assertThat(tables).containsExactly(
                new YashanDbMetadataReader.TableMetadata(new TableId(null, "APP", "ORDERS"), "TABLE"),
                new YashanDbMetadataReader.TableMetadata(new TableId(null, "APP", "CUSTOMERS"), "TABLE"));
        verify(statement).setString(1, "APP");
        verify(rows).close();
    }

    private static ResultSet rows(String[]... values) throws SQLException {
        ResultSet result = mock(ResultSet.class);
        AtomicInteger row = new AtomicInteger(-1);
        when(result.next()).thenAnswer(invocation -> row.incrementAndGet() < values.length);
        when(result.getString(anyInt())).thenAnswer(invocation -> values[row.get()][invocation.<Integer> getArgument(0) - 1]);
        return result;
    }
}
