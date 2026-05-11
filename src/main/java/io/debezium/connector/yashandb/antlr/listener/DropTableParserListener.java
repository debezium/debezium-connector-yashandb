/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.antlr.listener;

import io.debezium.connector.yashandb.antlr.YashanDbDdlParser;
import io.debezium.connector.yashandb.ddl.parser.gen.YashanDbParser;
import io.debezium.relational.TableId;

/**
 * This class is parsing YashanDB drop table statements.
 * Dropping a table removes it from the schema and generates a schema change event.
 */
public class DropTableParserListener extends BaseParserListener {

    private String catalogName;
    private String schemaName;
    private YashanDbDdlParser parser;

    /**
     * Creates a new DropTableParserListener.
     *
     * @param catalogName the catalog (database) name
     * @param schemaName the schema name
     * @param parser the parent DDL parser
     */
    DropTableParserListener(final String catalogName, final String schemaName, final YashanDbDdlParser parser) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
    }

    /**
     * Called when entering the drop_table_statement parse tree node.
     * Removes the table from the schema and signals the drop table event.
     *
     * @param ctx the drop_table_statement parse context
     */
    @Override
    public void enterDrop_table_statement(final YashanDbParser.Drop_table_statementContext ctx) {
        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.table_name()));
        parser.databaseTables().removeTable(tableId);
        parser.signalDropTable(tableId, ctx);
        super.enterDrop_table_statement(ctx);
    }
}