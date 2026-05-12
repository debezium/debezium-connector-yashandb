/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.antlr.listener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.ProxyParseTreeListenerUtil;
import io.debezium.connector.yashandb.antlr.YashanDbDdlParser;
import io.debezium.connector.yashandb.ddl.parser.gen.YashanDbParserBaseListener;
import io.debezium.text.ParsingException;

/**
 * This class is the main YashanDB DDL parser listener class.
 * It instantiates supported listeners, walks listeners through every parsing rule and collects parsing exceptions.
 *
 */
public class YashanDbDdlParserListener extends YashanDbParserBaseListener implements AntlrDdlParserListener {

    private final List<ParseTreeListener> listeners = new CopyOnWriteArrayList<>();
    private final Collection<ParsingException> errors = new ArrayList<>();

    public YashanDbDdlParserListener(final String catalogName, final String schemaName,
                                     final YashanDbDdlParser parser) {
        listeners.add(new CreateTableParserListener(catalogName, schemaName, parser, listeners));
        listeners.add(new AlterTableParserListener(catalogName, schemaName, parser, listeners));
        listeners.add(new DropTableParserListener(catalogName, schemaName, parser));
        listeners.add(new CommentParserListener(catalogName, schemaName, parser));
        listeners.add(new TruncateTableParserListener(catalogName, schemaName, parser));
    }

    @Override
    public Collection<ParsingException> getErrors() {
        return errors;
    }

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {
        ProxyParseTreeListenerUtil.delegateEnterRule(ctx, listeners, errors);
    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        ProxyParseTreeListenerUtil.delegateExitRule(ctx, listeners, errors);
    }
}
