/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.connector.yashandb.YashanDbValueConverters;
import io.debezium.connector.yashandb.antlr.YashanDbMySqlDdlParser;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;

/** MySQL-compatible data SQL over the YasDB JDBC connection. */
public final class YashanDbMySqlDialect implements YashanDbDialect {

    @Override
    public String name() {
        return "mysql";
    }

    @Override
    public String quoteCharacter() {
        return String.valueOf((char) 96);
    }

    @Override
    public String qualifiedTableName(TableId tableId) {
        String database = tableId.schema();
        return database == null || database.isEmpty() ? quote(tableId.table()) : quote(database) + "." + quote(tableId.table());
    }

    @Override
    public String lockTableStatement(String tableId) {
        return "LOCK TABLES " + tableId + " READ";
    }

    @Override
    public String lockTablesStatement(String tableId, Collection<TableId> capturedTables) {
        if (capturedTables.isEmpty()) {
            return lockTableStatement(tableId);
        }
        String tables = capturedTables.stream()
                .map(table -> qualifiedTableName(table) + " READ")
                .collect(Collectors.joining(", "));
        return "LOCK TABLES " + tables;
    }

    public String quote(String identifier) {
        String marker = quoteCharacter();
        return marker + identifier.replace(marker, marker + marker) + marker;
    }

    @Override
    public AntlrDdlParser<?, ?> createDdlParser(boolean includeComments, YashanDbValueConverters valueConverters, TableFilter tableFilter) {
        return new YashanDbMySqlDdlParser(true, includeComments, tableFilter);
    }

    @Override
    public String tableIdsQuery() {
        return "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'";
    }

    @Override
    public String tableIdsQuery(String schemaName) {
        return tableIdsQuery() + (schemaName == null ? "" : " AND table_schema = ?");
    }

    @Override
    public String columnsQuery(int tableCount) {
        return "SELECT table_schema, table_name, column_name, column_type, "
                + "character_maximum_length, numeric_precision, numeric_scale, is_nullable, column_default, "
                + "ordinal_position, extra FROM information_schema.columns "
                + "WHERE table_schema = ? AND table_name IN (" + placeholders(tableCount) + ") "
                + "ORDER BY table_schema, table_name, ordinal_position";
    }

    String primaryKeyOrUniqueIndexQuery(int tableCount) {
        return "SELECT table_schema, table_name, index_name, "
                + "CASE WHEN index_name = 'PRIMARY' THEN 'P' ELSE 'U' END AS key_type, column_name, seq_in_index "
                + "FROM information_schema.statistics WHERE table_schema = ? AND table_name IN (" + placeholders(tableCount) + ") "
                + "AND non_unique = 0 ORDER BY table_name, CASE WHEN index_name = 'PRIMARY' THEN 0 ELSE 1 END, index_name, seq_in_index";
    }

    String primaryKeyQuery() {
        return "SELECT column_name FROM information_schema.key_column_usage "
                + "WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY' ORDER BY ordinal_position";
    }

    String uniqueIndexQuery() {
        return "SELECT index_name, column_name, seq_in_index "
                + "FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? "
                + "AND non_unique = 0 AND index_name <> 'PRIMARY' ORDER BY index_name, seq_in_index";
    }

    static String placeholders(int count) {
        return String.join(", ", Collections.nCopies(count, "?"));
    }

    @Override
    public String latestTableDdlScnQuery(Collection<TableId> tables) {
        return "SELECT TIMESTAMP_TO_SCN(MAX(to_timestamp(update_time))) FROM INFORMATION_SCHEMA.tables WHERE "
                + tables.stream().map(table -> "(table_schema = ? AND table_name = ?)")
                        .collect(Collectors.joining(" OR "));
    }

    @Override
    public String tableDdlQuery(TableId tableId) {
        return "SHOW CREATE TABLE " + qualifiedTableName(tableId);
    }

    @Override
    public int tableDdlColumn() {
        return 2;
    }

    @Override
    public String limitedSelect(TableId tableId, int limit, String projection, Optional<String> condition,
                                Optional<String> additionalCondition, String orderBy) {
        return YashanDbNativeDialect.select(qualifiedTableName(tableId), projection, condition, additionalCondition, orderBy)
                + " LIMIT " + limit;
    }
}
