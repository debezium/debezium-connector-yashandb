/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yashandb.antlr.mysql.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.yashandb.antlr.YashanDbMySqlDdlParser;
import io.debezium.ddl.parser.yashandb.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.yashandb.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.TableId;

/**
 * Parser listener that is parsing MySQL RENAME TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class RenameTableParserListener extends MySqlParserBaseListener {

    private final static Logger LOG = LoggerFactory.getLogger(RenameTableParserListener.class);

    private final YashanDbMySqlDdlParser parser;

    public RenameTableParserListener(YashanDbMySqlDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterRenameTableClause(MySqlParser.RenameTableClauseContext ctx) {
        TableId oldTable = parser.parseQualifiedTableId(ctx.tableName(0).fullId());
        TableId newTable = parser.parseQualifiedTableId(ctx.tableName(1).fullId());
        if (parser.databaseTables().forTable(oldTable) == null) {
            return;
        }
        if (parser.getTableFilter().isIncluded(oldTable) && !parser.getTableFilter().isIncluded(newTable)) {
            LOG.warn("Renaming included table {} to non-included table {}, this can lead to schema inconsistency", oldTable, newTable);
        }
        else if (!parser.getTableFilter().isIncluded(oldTable) && parser.getTableFilter().isIncluded(newTable)) {
            LOG.warn("Renaming non-included table {} to included table {}, this can lead to schema inconsistency", oldTable, newTable);
        }
        parser.databaseTables().renameTable(oldTable, newTable);
        parser.signalAlterTable(newTable, oldTable, ctx);
        super.enterRenameTableClause(ctx);
    }
}
