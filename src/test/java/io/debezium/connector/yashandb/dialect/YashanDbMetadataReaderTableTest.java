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
    void shouldSelectMetadataReaderOnlyForMysqlMode() {
        assertThat(new YashanDbModeContext(false).metadataReader()).isNull();
        assertThat(new YashanDbModeContext(true).metadataReader()).isInstanceOf(YashanDbMySqlMetadataReader.class);
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
