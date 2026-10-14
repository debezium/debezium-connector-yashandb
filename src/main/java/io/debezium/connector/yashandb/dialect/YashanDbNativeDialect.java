/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
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
