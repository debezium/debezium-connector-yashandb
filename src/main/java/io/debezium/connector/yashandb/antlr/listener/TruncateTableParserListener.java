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
 * This class is parsing YashanDB truncate table statements.
 */
public class TruncateTableParserListener extends BaseParserListener {

    private final String catalogName;
    private final String schemaName;
    private final YashanDbDdlParser parser;

    TruncateTableParserListener(final String catalogName, final String schemaName, final YashanDbDdlParser parser) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
    }

    @Override
    public void enterTruncate_table_statement(final YashanDbParser.Truncate_table_statementContext ctx) {
        TableId tableId = new TableId(catalogName, schemaName, getTableNameFromTruncate(ctx));
        parser.signalTruncateTable(tableId, ctx);
        super.enterTruncate_table_statement(ctx);
    }

    private String getTableNameFromTruncate(YashanDbParser.Truncate_table_statementContext ctx) {
        // truncate_table_statement: TRUNCATE TABLE identifier | TRUNCATE TABLE schema '.' identifier
        if (ctx.schema() != null) {
            // schema.identifier format - return the table name (identifier after schema)
            return ctx.identifier().id_expression().getText();
        }
        else if (ctx.identifier() != null) {
            return ctx.identifier().id_expression().getText();
        }
        return ctx.getText();
    }
}