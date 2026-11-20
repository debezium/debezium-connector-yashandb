/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.connector.yashandb.dialect.YashanDbNativeDialect;
import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;

class AbstractStreamingAdapterTest {

    @FixFor("debezium/dbz#3014")
    @Test
    void latestTableDdlScnQueriesCapturedTablesInBoundedBatches() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement firstStatement = mock(PreparedStatement.class);
        PreparedStatement secondStatement = mock(PreparedStatement.class);
        ResultSet firstResult = mock(ResultSet.class);
        ResultSet secondResult = mock(ResultSet.class);
        when(connection.prepareStatement(anyString())).thenReturn(firstStatement, secondStatement);
        when(firstStatement.executeQuery()).thenReturn(firstResult);
        when(secondStatement.executeQuery()).thenReturn(secondResult);
        when(firstResult.next()).thenReturn(true);
        when(firstResult.getString(1)).thenReturn("100");
        when(secondResult.next()).thenReturn(true);
        when(secondResult.getString(1)).thenReturn("200");
        List<TableId> tables = new ArrayList<>();
        for (int i = 0; i < 1001; i++) {
            tables.add(new TableId("", "APP", "TABLE_" + i));
        }

        Optional<Scn> result = AbstractStreamingAdapter.queryLatestTableDdlScn(tables, new YashanDbNativeDialect(), connection);

        assertThat(result).contains(Scn.valueOf("200"));
        ArgumentCaptor<String> queries = ArgumentCaptor.forClass(String.class);
        verify(connection, times(2)).prepareStatement(queries.capture());
        assertThat(queries.getAllValues().get(0).chars().filter(character -> character == '?').count()).isEqualTo(2000);
        assertThat(queries.getAllValues().get(1).chars().filter(character -> character == '?').count()).isEqualTo(2);
    }
}
