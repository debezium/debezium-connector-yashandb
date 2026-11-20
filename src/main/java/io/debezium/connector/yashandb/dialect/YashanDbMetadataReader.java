/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;
import java.util.function.UnaryOperator;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;

/** Reads and maps table metadata for one database mode. */
public interface YashanDbMetadataReader {

    record TableMetadata(TableId id, String type) {
    }

    List<TableMetadata> readTables(JdbcConnection connection, String catalogName, String schemaName) throws SQLException;

    Map<TableId, List<Column>> readColumns(JdbcConnection connection, Collection<TableId> tableIds, ColumnNameFilter columnFilter,
                                           UnaryOperator<ColumnEditor> columnOverride, ToIntFunction<String> nativeTypeResolver)
            throws SQLException;

    Map<TableId, List<String>> readPrimaryKeyOrUniqueIndexNames(JdbcConnection connection, Collection<TableId> tableIds,
                                                                BiPredicate<String, String> indexFilter)
            throws SQLException;

    default List<String> readPrimaryKeyNames(JdbcConnection connection, TableId tableId, JdbcKeyMetadataReader jdbcReader) throws SQLException {
        return jdbcReader.read();
    }

    default List<String> readTableUniqueIndices(JdbcConnection connection, TableId tableId, BiPredicate<String, String> indexFilter,
                                                JdbcKeyMetadataReader jdbcReader)
            throws SQLException {
        return jdbcReader.read();
    }

    @FunctionalInterface
    interface JdbcKeyMetadataReader {
        List<String> read() throws SQLException;
    }
}
