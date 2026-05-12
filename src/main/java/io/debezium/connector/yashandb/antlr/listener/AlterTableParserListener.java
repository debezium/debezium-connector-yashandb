/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.antlr.listener;

import static io.debezium.antlr.AntlrDdlParser.getText;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.yashandb.antlr.YashanDbDdlParser;
import io.debezium.connector.yashandb.ddl.parser.gen.YashanDbParser;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;

/**
 * Parser listener that is parsing YashanDB ALTER TABLE statements
 */
public class AlterTableParserListener extends BaseParserListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlterTableParserListener.class);

    private static final int STARTING_INDEX = 1;
    private TableEditor tableEditor;
    private String catalogName;
    private String schemaName;
    private TableId previousTableId;
    private YashanDbDdlParser parser;
    private final List<ParseTreeListener> listeners;
    private ColumnDefinitionParserListener columnDefinitionParserListener;
    private List<ColumnEditor> columnEditors;
    private int parsingColumnIndex = STARTING_INDEX;

    /**
     * Package visible Constructor
     *
     * @param catalogName Represents database name. If null, points to the current database
     * @param schemaName Schema/user name. If null, points to the current schema
     * @param parser YashanDB Antlr parser
     * @param listeners registered listeners
     */
    AlterTableParserListener(final String catalogName, final String schemaName, final YashanDbDdlParser parser,
                             final List<ParseTreeListener> listeners) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
        this.listeners = listeners;
    }

    @Override
    public void enterAlter_table_statement(YashanDbParser.Alter_table_statementContext ctx) {
        previousTableId = null;

        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.table_name()));
        if (parser.databaseTables().forTable(tableId) == null) {
            LOGGER.debug("Ignoring ALTER TABLE statement for non-captured table {}", tableId);
            return;
        }
        tableEditor = parser.databaseTables().editTable(tableId);
        if (tableEditor == null) {
            throw new ParsingException(null, "Trying to alter table " + tableId.toString()
                    + ", which does not exist. Query: " + getText(ctx));
        }
        super.enterAlter_table_statement(ctx);
    }

    @Override
    public void exitAlter_table_statement(YashanDbParser.Alter_table_statementContext ctx) {
        parser.runIfNotNull(() -> {
            listeners.remove(columnDefinitionParserListener);
            parser.databaseTables().overwriteTable(tableEditor.create());
            parser.signalAlterTable(tableEditor.tableId(), previousTableId, ctx.getParent());
        }, tableEditor);
        super.exitAlter_table_statement(ctx);
    }

    @Override
    public void enterRename_clause(YashanDbParser.Rename_clauseContext ctx) {
        parser.runIfNotNull(() -> {
            previousTableId = tableEditor.tableId();
            String tableName = getTableName(ctx.table_name());
            final TableId newTableId = new TableId(tableEditor.tableId().catalog(), tableEditor.tableId().schema(), tableName);
            if (parser.getTableFilter().isIncluded(previousTableId) && !parser.getTableFilter().isIncluded(newTableId)) {
                LOGGER.warn("Renaming included table {} to non-included table {}, this can lead to schema inconsistency", previousTableId, newTableId);
            }
            else if (!parser.getTableFilter().isIncluded(previousTableId) && parser.getTableFilter().isIncluded(newTableId)) {
                LOGGER.warn("Renaming non-included table {} to included table {}, this can lead to schema inconsistency", previousTableId, newTableId);
            }
            parser.databaseTables().overwriteTable(tableEditor.create());
            parser.databaseTables().renameTable(tableEditor.tableId(), newTableId);
            tableEditor = parser.databaseTables().editTable(newTableId);
        }, tableEditor);
        super.enterRename_clause(ctx);
    }

    @Override
    public void enterAlter_column_clause(YashanDbParser.Alter_column_clauseContext ctx) {
        parser.runIfNotNull(() -> {
            // Handle ADD COLUMN
            if (ctx.ADD() != null) {
                YashanDbParser.Column_definitionContext columnDef = ctx.column_definition();
                if (columnDef != null) {
                    String columnName = getColumnName(columnDef.column_name());
                    ColumnEditor editor = Column.editor().name(columnName);
                    columnEditors = new ArrayList<>(1);
                    columnEditors.add(editor);
                    columnDefinitionParserListener = new ColumnDefinitionParserListener(tableEditor, editor, parser, listeners);
                    listeners.add(columnDefinitionParserListener);
                }
            }
            // Handle MODIFY COLUMN
            else if (ctx.MODIFY() != null) {
                YashanDbParser.Column_definitionContext columnDef = ctx.column_definition();
                if (columnDef != null) {
                    String columnName = getColumnName(columnDef.column_name());
                    Column existingColumn = tableEditor.columnWithName(columnName);
                    if (existingColumn != null) {
                        ColumnEditor columnEditor = existingColumn.edit();
                        columnEditors = new ArrayList<>(1);
                        columnEditors.add(columnEditor);
                        columnDefinitionParserListener = new ColumnDefinitionParserListener(tableEditor, columnEditor, parser, listeners);
                        listeners.add(columnDefinitionParserListener);
                    }
                    else {
                        throw new ParsingException(null, "trying to change column " + columnName + " in " +
                                tableEditor.tableId().toString() + " table, which does not exist.  Query: " + getText(ctx));
                    }
                }
            }
            // Handle RENAME COLUMN
            else if (ctx.RENAME() != null && ctx.COLUMN() != null) {
                List<YashanDbParser.Column_nameContext> columnNames = ctx.column_name();
                if (columnNames.size() >= 2) {
                    String oldColumnName = getColumnName(columnNames.get(0));
                    String newColumnName = getColumnName(columnNames.get(1));
                    tableEditor.renameColumn(oldColumnName, newColumnName);
                }
            }
            // Handle DROP COLUMN
            else if (ctx.DROP() != null) {
                List<YashanDbParser.Column_nameContext> columnNames = ctx.column_name();
                for (YashanDbParser.Column_nameContext columnNameContext : columnNames) {
                    String columnName = getColumnName(columnNameContext);
                    tableEditor.removeColumn(columnName);
                }
            }
        }, tableEditor);
        super.enterAlter_column_clause(ctx);
    }

    @Override
    public void exitAlter_column_clause(YashanDbParser.Alter_column_clauseContext ctx) {
        parser.runIfNotNull(() -> {
            if (columnEditors != null && (ctx.ADD() != null || ctx.MODIFY() != null)) {
                columnEditors.forEach(columnEditor -> tableEditor.addColumn(columnEditor.create()));
                listeners.remove(columnDefinitionParserListener);
                columnDefinitionParserListener = null;
                columnEditors = null;
            }
        }, tableEditor);
        super.exitAlter_column_clause(ctx);
    }

    @Override
    public void exitColumn_definition(YashanDbParser.Column_definitionContext ctx) {
        parser.runIfNotNull(() -> {
            if (columnEditors != null) {
                // column editor list is not null when a multiple columns are parsed in one statement
                if (columnEditors.size() > parsingColumnIndex) {
                    // assign next column editor to parse another column definition
                    columnDefinitionParserListener.setColumnEditor(columnEditors.get(parsingColumnIndex++));
                }
                else {
                    // all columns parsed
                    // reset global variables for next parsed statement
                    columnEditors.forEach(columnEditor -> tableEditor.addColumn(columnEditor.create()));
                    columnEditors = null;
                    parsingColumnIndex = STARTING_INDEX;
                }
            }
        }, tableEditor, columnEditors);
        super.exitColumn_definition(ctx);
    }

    @Override
    public void enterAlter_constraint_clause(YashanDbParser.Alter_constraint_clauseContext ctx) {
        parser.runIfNotNull(() -> {
            if (ctx.ADD() != null) {
                // ALTER TABLE ADD PRIMARY KEY
                if (ctx.PRIMARY() != null && ctx.KEY() != null) {
                    List<String> primaryKeyColumns = new ArrayList<>();
                    for (YashanDbParser.Column_nameContext columnNameContext : ctx.column_name()) {
                        primaryKeyColumns.add(getColumnName(columnNameContext));
                    }
                    if (!primaryKeyColumns.isEmpty()) {
                        tableEditor.setPrimaryKeyNames(primaryKeyColumns);
                    }
                }
            }
            else if (ctx.DROP() != null && ctx.PRIMARY() != null && ctx.KEY() != null) {
                // ALTER TABLE DROP PRIMARY KEY
                tableEditor.setPrimaryKeyNames(new ArrayList<>());
            }
        }, tableEditor);
        super.enterAlter_constraint_clause(ctx);
    }
}