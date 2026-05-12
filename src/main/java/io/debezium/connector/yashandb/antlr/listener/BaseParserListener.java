/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.antlr.listener;

import io.debezium.connector.yashandb.ddl.parser.gen.YashanDbParser;
import io.debezium.connector.yashandb.ddl.parser.gen.YashanDbParserBaseListener;

/**
 * This class contains common methods for all listeners
 */
class BaseParserListener extends YashanDbParserBaseListener {

    String getTableName(final YashanDbParser.Table_nameContext table_name) {
        // table_name: identifier | schema '.' identifier
        if (table_name.schema() != null) {
            // schema '.' identifier format - return the table name (identifier after schema)
            return getTableOrColumnName(table_name.identifier().id_expression().getText());
        }
        else if (table_name.identifier() != null) {
            return getTableOrColumnName(table_name.identifier().id_expression().getText());
        }
        return getTableOrColumnName(table_name.getText());
    }

    String getTableName(final YashanDbParser.Tableview_nameContext tableview_name) {
        // tableview_name: (identifier '.')? identifier (...)
        // Get the actual table/view name (last identifier part)
        if (!tableview_name.identifier().isEmpty()) {
            // Get the last identifier (the actual table name)
            return getTableOrColumnName(tableview_name.identifier(tableview_name.identifier().size() - 1).id_expression().getText());
        }
        return getTableOrColumnName(tableview_name.getText());
    }

    String getTableName(final YashanDbParser.Column_nameContext ctx) {
        // column_name: (identifier '.')? pseudo_column or (PRIOR|CONNECT_BY_ROOT)? identifier ('.' column_field)*
        if (ctx.identifier() != null) {
            return getTableOrColumnName(ctx.identifier().id_expression().getText());
        }
        return getTableOrColumnName(ctx.getText());
    }

    String getColumnName(final YashanDbParser.Column_nameContext ctx) {
        // column_name: (identifier '.')? pseudo_column or (PRIOR|CONNECT_BY_ROOT)? identifier ('.' column_field)*
        // Get the actual column name (last meaningful part)
        if (ctx.pseudo_column() != null) {
            return getTableOrColumnName(ctx.pseudo_column().getText());
        }
        else if (ctx.identifier() != null) {
            return getTableOrColumnName(ctx.identifier().id_expression().getText());
        }
        // Check for column_field which contains the actual column name
        if (!ctx.column_field().isEmpty()) {
            // Return the last column_field
            return getTableOrColumnName(ctx.column_field().get(ctx.column_field().size() - 1).getText());
        }
        return getTableOrColumnName(ctx.getText());
    }

    /**
     * Resolves a table or column name from the provided string.
     *
     * YashanDB table and column names are inherently stored in upper-case; however, if the objects
     * are created using double-quotes, the case of the object name is retained.  Therefore when
     * needing to parse a table or column name, this method will adhere to those rules and will
     * always return the name in upper-case unless the provided name is double-quoted in which
     * the returned value will have the double-quotes removed and case retained.
     *
     * @param name table or column name
     * @return parsed table or column name from the supplied name argument
     */
    static String getTableOrColumnName(String name) {
        return removeQuotes(name, true);
    }

    /**
     * Removes leading and trailing double quote characters from the provided string.
     *
     * @param text value to have double quotes removed
     * @param upperCaseIfNotQuoted control if returned string is upper-cased if not quoted
     * @return string that has had quotes removed
     */
    @SuppressWarnings("SameParameterValue")
    private static String removeQuotes(String text, boolean upperCaseIfNotQuoted) {
        if (text != null && text.length() > 2 && text.startsWith("\"") && text.endsWith("\"")) {
            return text.substring(1, text.length() - 1);
        }
        return upperCaseIfNotQuoted ? text.toUpperCase() : text;
    }
}