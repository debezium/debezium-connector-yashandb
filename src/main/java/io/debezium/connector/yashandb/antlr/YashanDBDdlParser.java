/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.antlr;

import com.yashandb.jdbc.YasTypes;
import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.antlr.DataTypeResolver.DataTypeEntry;
import io.debezium.connector.yashandb.YashanDBValueConverters;
import io.debezium.connector.yashandb.antlr.listener.YashanDBDdlParserListener;
import io.debezium.connector.yashandb.ddl.parser.gen.YashanDBLexer;
import io.debezium.connector.yashandb.ddl.parser.gen.YashanDBParser;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.Types;
import java.util.Arrays;

/**
 * This is the main YashanDB Antlr DDL parser
 */
public class YashanDBDdlParser extends AntlrDdlParser<YashanDBLexer, YashanDBParser> {

    private final TableFilter tableFilter;
    private final YashanDBValueConverters converters;

    private String catalogName;
    private String schemaName;

    public YashanDBDdlParser() {
        this(null, TableFilter.includeAll());
    }

    public YashanDBDdlParser(YashanDBValueConverters valueConverters) {
        this(true, valueConverters, TableFilter.includeAll());
    }

    public YashanDBDdlParser(YashanDBValueConverters valueConverters, TableFilter tableFilter) {
        this(true, valueConverters, tableFilter);
    }

    public YashanDBDdlParser(boolean throwErrorsFromTreeWalk, YashanDBValueConverters converters, TableFilter tableFilter) {
        this(throwErrorsFromTreeWalk, false, false, converters, tableFilter);
    }

    public YashanDBDdlParser(boolean throwErrorsFromTreeWalk, boolean includeViews, boolean includeComments,
                             YashanDBValueConverters converters, TableFilter tableFilter) {
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
    public ParseTree parseTree(YashanDBParser parser) {
        return parser.sql_script();
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new YashanDBDdlParserListener(catalogName, schemaName, this);
    }

    @Override
    protected YashanDBLexer createNewLexerInstance(CharStream charStreams) {
        return new YashanDBLexer(charStreams);
    }

    @Override
    protected YashanDBParser createNewParserInstance(CommonTokenStream commonTokenStream) {
        return new YashanDBParser(commonTokenStream);
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
                YashanDBParser.Native_datatype_elementContext.class.getCanonicalName(), Arrays.asList(
                        new DataTypeEntry(Types.INTEGER, YashanDBParser.INT),
                        new DataTypeEntry(Types.INTEGER, YashanDBParser.INTEGER),
                        new DataTypeEntry(Types.SMALLINT, YashanDBParser.SMALLINT),
                        new DataTypeEntry(Types.TINYINT, YashanDBParser.TINYINT),
                        new DataTypeEntry(Types.NUMERIC, YashanDBParser.NUMERIC),
                        new DataTypeEntry(Types.DECIMAL, YashanDBParser.DECIMAL),
                        new DataTypeEntry(Types.NUMERIC, YashanDBParser.NUMBER),
                        new DataTypeEntry(Types.REAL, YashanDBParser.REAL),
                        new DataTypeEntry(Types.DOUBLE, YashanDBParser.DOUBLE),
                        new DataTypeEntry(Types.BIGINT, YashanDBParser.BIGINT),
                        new DataTypeEntry(Types.BIT, YashanDBParser.BINARY),
                        new DataTypeEntry(Types.BINARY, YashanDBParser.BINARY),
                        new DataTypeEntry(Types.BOOLEAN, YashanDBParser.BOOLEAN),

                        new DataTypeEntry(Types.TIMESTAMP, YashanDBParser.DATE),
                        new DataTypeEntry(YasTypes.TIMESTAMP_LTZ, YashanDBParser.TIMESTAMP),
                        new DataTypeEntry(YasTypes.TIMESTAMP_TZ, YashanDBParser.TIMESTAMP),
                        new DataTypeEntry(Types.TIMESTAMP, YashanDBParser.TIMESTAMP),
                        new DataTypeEntry(Types.TIME, YashanDBParser.TIME),
                        new DataTypeEntry(Types.DATE, YashanDBParser.DATE),

                        new DataTypeEntry(Types.VARCHAR, YashanDBParser.VARCHAR2),
                        new DataTypeEntry(Types.VARCHAR, YashanDBParser.VARCHAR),
                        new DataTypeEntry(Types.NVARCHAR, YashanDBParser.NVARCHAR2),
                        new DataTypeEntry(Types.CHAR, YashanDBParser.CHAR),
                        new DataTypeEntry(Types.NCHAR, YashanDBParser.NCHAR),

                        new DataTypeEntry(Types.FLOAT, YashanDBParser.FLOAT),
                        new DataTypeEntry(Types.FLOAT, YashanDBParser.REAL),
                        new DataTypeEntry(Types.BLOB, YashanDBParser.BLOB),
                        new DataTypeEntry(Types.CLOB, YashanDBParser.CLOB)));
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

    public YashanDBValueConverters getConverters() {
        return converters;
    }

    public TableFilter getTableFilter() {
        return tableFilter;
    }
}
