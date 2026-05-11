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
 * This class contains the main YashanDB Antlr DDL parser.
 * It is used to parse DDL statements and update the relational database model.
 */
public class YashanDbDdlParser extends AntlrDdlParser<YashanDbLexer, YashanDbParser> {

    private final TableFilter tableFilter;
    private final YashanDbValueConverters converters;

    private String catalogName;
    private String schemaName;

    /**
     * Creates a new YashanDbDdlParser instance with default settings.
     * Error throwing is disabled, views and comments are not included.
     */
    public YashanDbDdlParser() {
        this(null, TableFilter.includeAll());
    }

    /**
     * Creates a new YashanDbDdlParser instance with the given value converters.
     *
     * @param valueConverters the value converters to use for data type conversion
     */
    public YashanDbDdlParser(YashanDbValueConverters valueConverters) {
        this(true, valueConverters, TableFilter.includeAll());
    }

    /**
     * Creates a new YashanDbDdlParser instance with the given value converters and table filter.
     *
     * @param valueConverters the value converters to use for data type conversion
     * @param tableFilter the filter to determine which tables to include
     */
    public YashanDbDdlParser(YashanDbValueConverters valueConverters, TableFilter tableFilter) {
        this(true, valueConverters, tableFilter);
    }

    /**
     * Creates a new YashanDbDdlParser instance with configurable error handling.
     *
     * @param throwErrorsFromTreeWalk whether to throw errors during tree walk
     * @param converters the value converters to use for data type conversion
     * @param tableFilter the filter to determine which tables to include
     */
    public YashanDbDdlParser(boolean throwErrorsFromTreeWalk, YashanDbValueConverters converters, TableFilter tableFilter) {
        this(throwErrorsFromTreeWalk, false, false, converters, tableFilter);
    }

    /**
     * Creates a new YashanDbDdlParser instance with full configuration.
     *
     * @param throwErrorsFromTreeWalk whether to throw errors during tree walk
     * @param includeViews whether to include views in parsing
     * @param includeComments whether to include comments in parsing
     * @param converters the value converters to use for data type conversion
     * @param tableFilter the filter to determine which tables to include
     */
    public YashanDbDdlParser(boolean throwErrorsFromTreeWalk, boolean includeViews, boolean includeComments,
                             YashanDbValueConverters converters, TableFilter tableFilter) {
        super(throwErrorsFromTreeWalk, includeViews, includeComments);
        this.converters = converters;
        this.tableFilter = tableFilter;
    }

    /**
     * Parses the given DDL content and updates the database tables model.
     * Ensures the DDL content ends with a semicolon before parsing.
     *
     * @param ddlContent the DDL content to parse
     * @param databaseTables the database tables model to update
     * @return the DDL changes detected during parsing
     */
    @Override
    public DdlChanges parse(String ddlContent, Tables databaseTables) {
        if (!ddlContent.endsWith(";")) {
            ddlContent = ddlContent + ";";
        }
        return super.parse(ddlContent, databaseTables);
    }

    /**
     * Returns the root of the parse tree for the YashanDB SQL script grammar.
     *
     * @param parser the parser to use for tree construction
     * @return the parse tree root node
     */
    @Override
    public ParseTree parseTree(YashanDbParser parser) {
        return parser.sql_script();
    }

    /**
     * Creates a new parse tree walker listener for YashanDB DDL processing.
     *
     * @return a new YashanDbDdlParserListener instance
     */
    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new YashanDbDdlParserListener(catalogName, schemaName, this);
    }

    /**
     * Creates a new lexer instance for the YashanDB grammar.
     *
     * @param charStreams the character stream input
     * @return a new YashanDbLexer instance
     */
    @Override
    protected YashanDbLexer createNewLexerInstance(CharStream charStreams) {
        return new YashanDbLexer(charStreams);
    }

    /**
     * Creates a new parser instance for the YashanDB grammar.
     *
     * @param commonTokenStream the token stream input
     * @return a new YashanDbParser instance
     */
    @Override
    protected YashanDbParser createNewParserInstance(CommonTokenStream commonTokenStream) {
        return new YashanDbParser(commonTokenStream);
    }

    /**
     * Indicates whether the YashanDB grammar uses upper-case tokens.
     *
     * @return true since YashanDB grammar tokens are in upper case
     */
    @Override
    protected boolean isGrammarInUpperCase() {
        return true;
    }

    /**
     * Returns the data type resolver for YashanDB native data types.
     * Maps YashanDB data type names to JDBC type codes.
     *
     * @return the data type resolver
     */
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

    /**
     * Creates a new system variables instance. Currently not implemented.
     *
     * @return null as system variables are not yet supported
     */
    @Override
    protected SystemVariables createNewSystemVariablesInstance() {
        // todo implement
        return null;
    }

    /**
     * Sets the current database (catalog) context for DDL parsing.
     *
     * @param databaseName the database name to set as current
     */
    @Override
    public void setCurrentDatabase(String databaseName) {
        this.catalogName = databaseName;
    }

    /**
     * Sets the current schema context for DDL parsing.
     *
     * @param schemaName the schema name to set as current
     */
    @Override
    public void setCurrentSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * Returns the system variables instance. Currently not implemented.
     *
     * @return never returns normally, throws UnsupportedOperationException
     */
    @Override
    public SystemVariables systemVariables() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Runs a function if all given objects are not null.
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

    /**
     * Returns the value converters used by this parser.
     *
     * @return the value converters instance
     */
    public YashanDbValueConverters getConverters() {
        return converters;
    }

    /**
     * Returns the table filter used to determine which tables to include.
     *
     * @return the table filter instance
     */
    public TableFilter getTableFilter() {
        return tableFilter;
    }
}
