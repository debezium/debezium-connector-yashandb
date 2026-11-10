/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Optional;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.connector.yashandb.YashanDbValueConverters;
import io.debezium.connector.yashandb.antlr.YashanDbDdlParser;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;

/** Existing YashanDB SQL behavior. */
public final class YashanDbNativeDialect implements YashanDbDialect {

    @Override
    public String name() {
        return "yashan";
    }

    @Override
    public String quoteCharacter() {
        return "\"";
    }

    @Override
    public String qualifiedTableName(TableId tableId) {
        return new TableId(tableId.catalog(), tableId.schema(), tableId.table()).toDoubleQuotedString();
    }

    @Override
    public String lockTableStatement(String tableId) {
        return String.format("LOCK TABLE %s IN SHARE MODE", tableId);
    }

    @Override
    public AntlrDdlParser<?, ?> createDdlParser(boolean includeComments, YashanDbValueConverters valueConverters, TableFilter tableFilter) {
        return new YashanDbDdlParser(true, false, includeComments, valueConverters, tableFilter);
    }

    @Override
    public String tableIdsQuery() {
        return "SELECT owner, table_name FROM all_tables";
    }

    @Override
    public String tableIdsQuery(String schemaName) {
        return tableIdsQuery() + (schemaName == null ? "" : " WHERE owner = ?");
    }

    @Override
    public String columnsQuery(int tableCount) {
        return "SELECT owner, table_name, column_name, data_type, data_length, char_length, "
                + "data_precision, data_scale, nullable, data_default, column_id FROM all_tab_cols "
                + "WHERE owner = ? AND table_name IN (" + placeholders(tableCount) + ") "
                + "AND user_generated = 'Y' ORDER BY owner, table_name, column_id";
    }

    String primaryKeyOrUniqueIndexQuery(int tableCount) {
        String tablePlaceholders = placeholders(tableCount);
        return "SELECT owner, table_name, key_name, key_type, column_name, position FROM ("
                + "SELECT c.owner, c.table_name, c.constraint_name AS key_name, 'P' AS key_type, "
                + "cc.column_name, cc.position FROM all_constraints c JOIN all_cons_columns cc "
                + "ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name AND cc.table_name = c.table_name "
                + "WHERE c.owner = ? AND c.constraint_type IN ('PRIMARY KEY', 'P') AND c.table_name IN (" + tablePlaceholders + ") "
                + "UNION ALL SELECT i.table_owner, i.table_name, i.index_name, 'U', ic.column_name, ic.column_position "
                + "FROM all_indexes i JOIN all_ind_columns ic ON ic.index_owner = i.owner AND ic.index_name = i.index_name "
                + "AND ic.table_owner = i.table_owner AND ic.table_name = i.table_name "
                + "WHERE i.table_owner = ? AND i.uniqueness = 'Y' AND i.table_name IN (" + tablePlaceholders + ")) "
                + "ORDER BY table_name, CASE key_type WHEN 'P' THEN 0 ELSE 1 END, key_name, position";
    }

    static String placeholders(int count) {
        return String.join(", ", Collections.nCopies(count, "?"));
    }

    @Override
    public String tableDdlQuery(TableId tableId) {
        return "SELECT dbms_metadata.get_ddl('TABLE',?,?) FROM DUAL";
    }

    @Override
    public boolean tableDdlUsesParameters() {
        return true;
    }

    @Override
    public void bindTableDdlQuery(PreparedStatement statement, TableId tableId) throws SQLException {
        statement.setString(1, tableId.table());
        statement.setString(2, tableId.schema());
    }

    @Override
    public int tableDdlColumn() {
        return 1;
    }

    @Override
    public String limitedSelect(TableId tableId, int limit, String projection, Optional<String> condition,
                                Optional<String> additionalCondition, String orderBy) {
        String sql = select(qualifiedTableName(tableId), projection, condition, additionalCondition, orderBy);
        return " SELECT * FROM (" + sql + ") WHERE ROWNUM <=" + limit;
    }

    static String select(String table, String projection, Optional<String> condition,
                         Optional<String> additionalCondition, String orderBy) {
        StringBuilder sql = new StringBuilder("SELECT ").append(projection).append(" FROM ").append(table);
        condition.ifPresent(value -> sql.append(" WHERE ").append(value));
        additionalCondition.ifPresent(value -> sql.append(condition.isPresent() ? " AND " : " WHERE ").append(value));
        return sql.append(" ORDER BY ").append(orderBy).toString();
    }
}
