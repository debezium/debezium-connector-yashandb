/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.connector.yashandb.YashanDbValueConverters;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;

/** SQL and identifier rules selected for a single connector task. */
public interface YashanDbDialect {

    String name();

    String quoteCharacter();

    default TableId tableId(String database, String schema, String table) {
        return new TableId(database, schema, table);
    }

    String qualifiedTableName(TableId tableId);

    /** Creates a parser for the dialect with the connector's comment and table filtering settings. */
    AntlrDdlParser<?, ?> createDdlParser(boolean includeComments, YashanDbValueConverters valueConverters, TableFilter tableFilter);

    String tableIdsQuery();

    /** Builds a table query with an optional schema filter for dialects using SQL metadata. */
    default String tableIdsQuery(String schemaName) {
        throw new UnsupportedOperationException("SQL table metadata is not supported by dialect " + name());
    }

    /** Returns column metadata SQL for one schema and the given number of table name parameters. */
    default String columnsQuery(int tableCount) {
        throw new UnsupportedOperationException("SQL column metadata is not supported by dialect " + name());
    }

    /** Builds the query used to find the latest DDL SCN for the supplied tables. */
    default String latestTableDdlScnQuery(Collection<TableId> tables) {
        return "SELECT TIMESTAMP_TO_SCN(MAX(to_timestamp(last_ddl_time))) FROM all_objects WHERE "
                + tables.stream().map(table -> "(owner = ? AND object_name = ?)").collect(Collectors.joining(" OR "));
    }

    /** Binds the table identifiers used by {@link #latestTableDdlScnQuery(Collection)}. */
    default void bindLatestTableDdlScnQuery(PreparedStatement statement, Collection<TableId> tables) throws SQLException {
        int parameterIndex = 1;
        for (TableId table : tables) {
            statement.setString(parameterIndex++, table.schema());
            statement.setString(parameterIndex++, table.table());
        }
    }

    String tableDdlQuery(TableId tableId);

    default boolean tableDdlUsesParameters() {
        return false;
    }

    /** Binds the table identifier values used by the DDL query, if the query has parameters. */
    default void bindTableDdlQuery(PreparedStatement statement, TableId tableId) throws SQLException {
    }

    int tableDdlColumn();

    String limitedSelect(TableId tableId, int limit, String projection, Optional<String> condition,
                         Optional<String> additionalCondition, String orderBy);

    default String snapshotSelect(String table, String columns, String scn) {
        return String.format("SELECT %s FROM %s AS OF SCN %s", columns, table, scn);
    }

    /** Returns the SQL that locks a single table, which is already quoted for this dialect. */
    String lockTableStatement(String tableId);

    /**
     * Returns the SQL that locks all snapshot tables. MySQL replaces existing session locks whenever
     * {@code LOCK TABLES} runs, so all tables must be locked by a single statement.
     */
    default String lockTablesStatement(String tableId, Collection<TableId> capturedTables) {
        return lockTableStatement(tableId);
    }
}
