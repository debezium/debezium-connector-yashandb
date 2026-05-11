/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.antlr.listener;

import java.util.List;
import java.util.stream.Collectors;

import io.debezium.connector.yashandb.antlr.YashanDbDdlParser;
import io.debezium.connector.yashandb.ddl.parser.gen.YashanDbParser;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

/**
 * This class is parsing YashanDB table and column comment statements.
 * Column comments are used to enrich the table column metadata.
 */
public class CommentParserListener extends BaseParserListener {
    private final String catalogName;
    private final String schemaName;
    private final YashanDbDdlParser parser;
    private TableEditor tableEditor;

    /**
     * Creates a new CommentParserListener.
     *
     * @param catalogName the catalog (database) name
     * @param schemaName the schema name
     * @param parser the parent DDL parser
     */
    CommentParserListener(final String catalogName, final String schemaName, final YashanDbDdlParser parser) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
    }

    /**
     * Called when entering the comment_statement parse tree node.
     * Parses COMMENT ON TABLE and COMMENT ON COLUMN statements.
     *
     * @param ctx the comment_statement parse context
     */
    @Override
    public void enterComment_statement(YashanDbParser.Comment_statementContext ctx) {
        if (!parser.skipComments()) {
            String comment = parser.withoutQuotes(ctx.quoted_string().getText());

            // COMMENT ON TABLE
            if (ctx.TABLE() != null) {
                String tableName = getTableNameFromComment(ctx);
                TableId tableId = new TableId(catalogName, schemaName, tableName);
                if (parser.getTableFilter().isIncluded(tableId)) {
                    if (parser.databaseTables().forTable(tableId) != null) {
                        tableEditor = parser.databaseTables().editTable(tableId);
                        parser.runIfNotNull(() -> {
                            tableEditor.setComment(comment);
                        }, tableEditor);
                    }
                }
            }
            // COMMENT ON COLUMN
            else if (ctx.COLUMN() != null) {
                List<YashanDbParser.IdentifierContext> identifiers = ctx.identifier();
                if (identifiers.size() >= 2) {
                    // identifier.identifier format - first is schema or table, second is table or column
                    String tableName;
                    String columnName;

                    if (identifiers.size() >= 4) {
                        // schema.table.column format
                        tableName = identifiers.get(1).id_expression().getText();
                        columnName = identifiers.get(3).id_expression().getText();
                    }
                    else if (identifiers.size() == 3) {
                        // schema.table.column format (schema '.' identifier '.' identifier)
                        tableName = identifiers.get(1).id_expression().getText();
                        columnName = identifiers.get(2).id_expression().getText();
                    }
                    else {
                        // table.column format
                        tableName = identifiers.get(0).id_expression().getText();
                        columnName = identifiers.get(1).id_expression().getText();
                    }

                    TableId tableId = new TableId(catalogName, schemaName, getTableOrColumnName(tableName));
                    if (parser.getTableFilter().isIncluded(tableId)) {
                        Table table = parser.databaseTables().forTable(tableId);
                        if (table != null) {
                            tableEditor = parser.databaseTables().editTable(tableId);
                            parser.runIfNotNull(() -> {
                                String column = getTableOrColumnName(columnName);
                                List<Column> columns = table.columns().stream()
                                        .map(m -> {
                                            if (m.name().equalsIgnoreCase(column)) {
                                                m = m.edit().comment(comment).create();
                                            }
                                            return m;
                                        })
                                        .collect(Collectors.toList());
                                tableEditor.setColumns(columns);
                            }, tableEditor);
                        }
                    }
                }
            }
        }
        super.enterComment_statement(ctx);
    }

    /**
     * Called when exiting the comment_statement parse tree node.
     * Finalizes and persists the table comment changes.
     *
     * @param ctx the comment_statement parse context
     */
    @Override
    public void exitComment_statement(YashanDbParser.Comment_statementContext ctx) {
        if (!parser.skipComments()) {
            parser.runIfNotNull(() -> {
                parser.databaseTables().overwriteTable(tableEditor.create());
                parser.signalCreateTable(tableEditor.tableId(), ctx);
            }, tableEditor);
        }
        super.exitComment_statement(ctx);
    }

    private String getTableNameFromComment(YashanDbParser.Comment_statementContext ctx) {
        List<YashanDbParser.IdentifierContext> identifiers = ctx.identifier();
        if (ctx.schema() != null) {
            // schema '.' identifier format
            if (identifiers.size() >= 1) {
                return identifiers.get(0).id_expression().getText();
            }
        }
        else if (identifiers.size() >= 1) {
            return identifiers.get(0).id_expression().getText();
        }
        return ctx.getText();
    }
}