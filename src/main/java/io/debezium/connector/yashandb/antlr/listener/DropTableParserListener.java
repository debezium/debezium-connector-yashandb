/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.antlr.listener;

import io.debezium.connector.yashandb.antlr.YashanDBDdlParser;
import io.debezium.connector.yashandb.ddl.parser.gen.YashanDBParser;
import io.debezium.relational.TableId;

/**
 * This class is parsing YashanDB drop table statements.
 */
public class DropTableParserListener extends BaseParserListener {

    private String catalogName;
    private String schemaName;
    private YashanDBDdlParser parser;

    DropTableParserListener(final String catalogName, final String schemaName, final YashanDBDdlParser parser) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
    }

    @Override
    public void enterDrop_table_statement(final YashanDBParser.Drop_table_statementContext ctx) {
        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.table_name()));
        parser.databaseTables().removeTable(tableId);
        parser.signalDropTable(tableId, ctx);
        super.enterDrop_table_statement(ctx);
    }
}