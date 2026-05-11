/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.antlr.listener;

import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.yashandb.antlr.YashanDbDdlParser;
import io.debezium.connector.yashandb.ddl.parser.gen.YashanDbParser;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;

/**
 * This class is parsing YashanDB create table statements.
 * It maps the table and column definitions to the relational table model.
 */
public class CreateTableParserListener extends BaseParserListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableParserListener.class);

    private final List<ParseTreeListener> listeners;
    private TableEditor tableEditor;
    private final String catalogName;
    private final String schemaName;
    private final YashanDbDdlParser parser;
    private ColumnDefinitionParserListener columnDefinitionParserListener;
    private String inlinePrimaryKey;

    /**
     * Creates a new CreateTableParserListener.
     *
     * @param catalogName the catalog (database) name
     * @param schemaName the schema name
     * @param parser the parent DDL parser
     * @param listeners the list of registered parse tree listeners
     */
    CreateTableParserListener(final String catalogName, final String schemaName, final YashanDbDdlParser parser,
                              final List<ParseTreeListener> listeners) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
        this.listeners = listeners;
    }

    /**
     * Called when entering the create_table_statement parse tree node.
     * Initializes the table editor for the new table.
     *
     * @param ctx the create_table_statement parse context
     */
    @Override
    public void enterCreate_table_statement(YashanDbParser.Create_table_statementContext ctx) {
        if (ctx.relation_properties() == null) {
            throw new ParsingException(null, "Only relational tables are supported");
        }
        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.table_name()));
        if (parser.getTableFilter().isIncluded(tableId)) {
            if (parser.databaseTables().forTable(tableId) == null) {
                tableEditor = parser.databaseTables().editOrCreateTable(tableId);
                super.enterCreate_table_statement(ctx);
            }
        }
        else {
            LOGGER.debug("Ignoring CREATE TABLE statement for non-captured table {}", tableId);
        }
    }

    /**
     * Called when exiting the create_table_statement parse tree node.
     * Finalizes table creation and signals the create table event.
     *
     * @param ctx the create_table_statement parse context
     */
    @Override
    public void exitCreate_table_statement(YashanDbParser.Create_table_statementContext ctx) {
        parser.runIfNotNull(() -> {
            if (inlinePrimaryKey != null) {
                if (!tableEditor.primaryKeyColumnNames().isEmpty()) {
                    throw new ParsingException(null, "Can only specify in-line or out-of-line primary keys but not both");
                }
                tableEditor.setPrimaryKeyNames(inlinePrimaryKey);
            }

            Table table = getTable();
            assert table != null;
            parser.runIfNotNull(() -> {
                listeners.remove(columnDefinitionParserListener);
                columnDefinitionParserListener = null;
                parser.databaseTables().overwriteTable(table);
                parser.signalCreateTable(tableEditor.tableId(), ctx);
            }, table);
        }, tableEditor);

        super.exitCreate_table_statement(ctx);
    }

    /**
     * Called when entering the column_definition parse tree node.
     * Creates a column editor and registers the column definition listener.
     *
     * @param ctx the column_definition parse context
     */
    @Override
    public void enterColumn_definition(YashanDbParser.Column_definitionContext ctx) {
        parser.runIfNotNull(() -> {
            String columnName = getColumnName(ctx.column_name());
            ColumnEditor columnEditor = Column.editor().name(columnName);
            if (columnDefinitionParserListener == null) {
                columnDefinitionParserListener = new ColumnDefinitionParserListener(tableEditor, columnEditor, parser, listeners);
                columnDefinitionParserListener.enterColumn_definition(ctx);
                listeners.add(columnDefinitionParserListener);
            }
            else {
                columnDefinitionParserListener.setColumnEditor(columnEditor);
            }
        }, tableEditor);
        super.enterColumn_definition(ctx);
    }

    /**
     * Called when exiting the column_definition parse tree node.
     * Adds the parsed column to the table editor.
     *
     * @param ctx the column_definition parse context
     */
    @Override
    public void exitColumn_definition(YashanDbParser.Column_definitionContext ctx) {
        parser.runIfNotNull(() -> tableEditor.addColumn(columnDefinitionParserListener.getColumn()),
                tableEditor, columnDefinitionParserListener);
        super.exitColumn_definition(ctx);
    }

    /**
     * Called when exiting the inline_constraint parse tree node.
     * Handles inline PRIMARY KEY constraint definitions.
     *
     * @param ctx the inline_constraint parse context
     */
    @Override
    public void exitInline_constraint(YashanDbParser.Inline_constraintContext ctx) {
        if (ctx.PRIMARY() != null) {
            if (ctx.getParent() instanceof YashanDbParser.Column_definitionContext) {
                YashanDbParser.Column_definitionContext columnCtx = (YashanDbParser.Column_definitionContext) ctx.getParent();
                inlinePrimaryKey = getColumnName(columnCtx.column_name());
            }
        }
        super.exitInline_constraint(ctx);
    }

    /**
     * Called when exiting the index_definition parse tree node.
     * Handles out-of-line PRIMARY KEY index definitions.
     *
     * @param ctx the index_definition parse context
     */
    @Override
    public void exitIndex_definition(YashanDbParser.Index_definitionContext ctx) {
        parser.runIfNotNull(() -> {
            if (ctx.PRIMARY() != null) {
                if (inlinePrimaryKey != null) {
                    throw new ParsingException(null, "Cannot specify inline and out of line primary keys");
                }
                List<String> pkColumnNames = ctx.column_name().stream()
                        .map(this::getColumnName)
                        .collect(Collectors.toList());

                tableEditor.setPrimaryKeyNames(pkColumnNames);
            }
        }, tableEditor);
        super.exitIndex_definition(ctx);
    }

    /**
     * Returns the created table from the table editor, or null if no editor exists.
     *
     * @return the table instance or null
     */
    private Table getTable() {
        return tableEditor != null ? tableEditor.create() : null;
    }
}