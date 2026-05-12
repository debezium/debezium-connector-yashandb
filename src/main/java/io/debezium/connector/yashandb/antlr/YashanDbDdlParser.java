/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.antlr;

import java.sql.Types;
import java.util.Arrays;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import com.yashandb.jdbc.YasTypes;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.antlr.DataTypeResolver.DataTypeEntry;
import io.debezium.connector.yashandb.YashanDbValueConverters;
import io.debezium.connector.yashandb.antlr.listener.YashanDbDdlParserListener;
import io.debezium.connector.yashandb.ddl.parser.gen.YashanDbLexer;
import io.debezium.connector.yashandb.ddl.parser.gen.YashanDbParser;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;

/**
 * This is the main YashanDB Antlr DDL parser
 */
public class YashanDbDdlParser extends AntlrDdlParser<YashanDbLexer, YashanDbParser> {

    private final TableFilter tableFilter;
    private final YashanDbValueConverters converters;

    private String catalogName;
    private String schemaName;

    public YashanDbDdlParser() {
        this(null, TableFilter.includeAll());
    }

    public YashanDbDdlParser(YashanDbValueConverters valueConverters) {
        this(true, valueConverters, TableFilter.includeAll());
    }

    public YashanDbDdlParser(YashanDbValueConverters valueConverters, TableFilter tableFilter) {
        this(true, valueConverters, tableFilter);
    }

    public YashanDbDdlParser(boolean throwErrorsFromTreeWalk, YashanDbValueConverters converters, TableFilter tableFilter) {
        this(throwErrorsFromTreeWalk, false, false, converters, tableFilter);
    }

    public YashanDbDdlParser(boolean throwErrorsFromTreeWalk, boolean includeViews, boolean includeComments,
                             YashanDbValueConverters converters, TableFilter tableFilter) {
        super(throwErrorsFromTreeWalk, includeViews, includeComments);
        this.converters = converters;
        this.tableFilter = tableFilter;
    }

    @Override
    public DdlChanges parse(String ddlContent, Tables databaseTables) {
        if (!ddlContent.endsWith(";")) {
            ddlContent = ddlContent + ";";
        }
        return super.parse(ddlContent, databaseTables);
    }

    @Override
    public ParseTree parseTree(YashanDbParser parser) {
        return parser.sql_script();
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new YashanDbDdlParserListener(catalogName, schemaName, this);
    }

    @Override
    protected YashanDbLexer createNewLexerInstance(CharStream charStreams) {
        return new YashanDbLexer(charStreams);
    }

    @Override
    protected YashanDbParser createNewParserInstance(CommonTokenStream commonTokenStream) {
        return new YashanDbParser(commonTokenStream);
    }

    @Override
    protected boolean isGrammarInUpperCase() {
        return true;
    }

    @Override
    public DataTypeResolver dataTypeResolver() {
        // todo, register all and use in ColumnDefinitionParserListener
        DataTypeResolver.Builder dataTypeResolverBuilder = new DataTypeResolver.Builder();

        dataTypeResolverBuilder.registerDataTypes(
                YashanDbParser.Native_datatype_elementContext.class.getCanonicalName(), Arrays.asList(
                        new DataTypeEntry(Types.INTEGER, YashanDbParser.INT),
                        new DataTypeEntry(Types.INTEGER, YashanDbParser.INTEGER),
                        new DataTypeEntry(Types.SMALLINT, YashanDbParser.SMALLINT),
                        new DataTypeEntry(Types.TINYINT, YashanDbParser.TINYINT),
                        new DataTypeEntry(Types.NUMERIC, YashanDbParser.NUMERIC),
                        new DataTypeEntry(Types.DECIMAL, YashanDbParser.DECIMAL),
                        new DataTypeEntry(Types.NUMERIC, YashanDbParser.NUMBER),
                        new DataTypeEntry(Types.REAL, YashanDbParser.REAL),
                        new DataTypeEntry(Types.DOUBLE, YashanDbParser.DOUBLE),
                        new DataTypeEntry(Types.BIGINT, YashanDbParser.BIGINT),
                        new DataTypeEntry(Types.BIT, YashanDbParser.BINARY),
                        new DataTypeEntry(Types.BINARY, YashanDbParser.BINARY),
                        new DataTypeEntry(Types.BOOLEAN, YashanDbParser.BOOLEAN),

                        new DataTypeEntry(Types.TIMESTAMP, YashanDbParser.DATE),
                        new DataTypeEntry(YasTypes.TIMESTAMP_LTZ, YashanDbParser.TIMESTAMP),
                        new DataTypeEntry(YasTypes.TIMESTAMP_TZ, YashanDbParser.TIMESTAMP),
                        new DataTypeEntry(Types.TIMESTAMP, YashanDbParser.TIMESTAMP),
                        new DataTypeEntry(Types.TIME, YashanDbParser.TIME),
                        new DataTypeEntry(Types.DATE, YashanDbParser.DATE),

                        new DataTypeEntry(Types.VARCHAR, YashanDbParser.VARCHAR2),
                        new DataTypeEntry(Types.VARCHAR, YashanDbParser.VARCHAR),
                        new DataTypeEntry(Types.NVARCHAR, YashanDbParser.NVARCHAR2),
                        new DataTypeEntry(Types.CHAR, YashanDbParser.CHAR),
                        new DataTypeEntry(Types.NCHAR, YashanDbParser.NCHAR),

                        new DataTypeEntry(Types.FLOAT, YashanDbParser.FLOAT),
                        new DataTypeEntry(Types.FLOAT, YashanDbParser.REAL),
                        new DataTypeEntry(Types.BLOB, YashanDbParser.BLOB),
                        new DataTypeEntry(Types.CLOB, YashanDbParser.CLOB)));
        return dataTypeResolverBuilder.build();
    }

    @Override
    protected SystemVariables createNewSystemVariablesInstance() {
        // todo implement
        return null;
    }

    @Override
    public void setCurrentDatabase(String databaseName) {
        this.catalogName = databaseName;
    }

    @Override
    public void setCurrentSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public SystemVariables systemVariables() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Runs a function if all given object are not null.
     *
     * @param function        function to run; may not be null
     * @param nullableObjects object to be tested, if they are null.
     */
    public void runIfNotNull(Runnable function, Object... nullableObjects) {
        for (Object nullableObject : nullableObjects) {
            if (nullableObject == null) {
                return;
            }
        }
        function.run();
    }

    public YashanDbValueConverters getConverters() {
        return converters;
    }

    public TableFilter getTableFilter() {
        return tableFilter;
    }
}
