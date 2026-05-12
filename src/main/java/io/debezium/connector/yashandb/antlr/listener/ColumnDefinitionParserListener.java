/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.antlr.listener;

import java.sql.Types;
import java.util.List;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import com.yashandb.jdbc.YasTypes;

import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.yashandb.antlr.YashanDbDdlParser;
import io.debezium.connector.yashandb.ddl.parser.gen.YashanDbParser;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;

/**
 * Parser listener that parses column definitions of YashanDB DDL statements.
 */
public class ColumnDefinitionParserListener extends BaseParserListener {

    private final YashanDbDdlParser parser;
    private final DataTypeResolver dataTypeResolver;
    private final TableEditor tableEditor;
    private final List<ParseTreeListener> listeners;
    private ColumnEditor columnEditor;

    ColumnDefinitionParserListener(final TableEditor tableEditor, final ColumnEditor columnEditor, YashanDbDdlParser parser,
                                   List<ParseTreeListener> listeners) {
        this.tableEditor = tableEditor;
        this.columnEditor = columnEditor;
        this.parser = parser;
        this.dataTypeResolver = parser.dataTypeResolver();
        this.listeners = listeners;
    }

    void setColumnEditor(ColumnEditor columnEditor) {
        this.columnEditor = columnEditor;
    }

    Column getColumn() {
        return columnEditor.create();
    }

    @Override
    public void enterColumn_definition(YashanDbParser.Column_definitionContext ctx) {
        resolveColumnDataType(ctx);
        if (ctx.column_constraint() != null) {
            for (YashanDbParser.Column_constraintContext constraintCtx : ctx.column_constraint()) {
                if (constraintCtx.DEFAULT() != null && constraintCtx.default_expr() != null) {
                    columnEditor.defaultValueExpression(constraintCtx.default_expr().getText());
                }
            }
        }
        super.enterColumn_definition(ctx);
    }

    private void resolveColumnDataType(YashanDbParser.Column_definitionContext ctx) {
        columnEditor.name(getColumnName(ctx.column_name()));

        boolean hasNotNullConstraint = ctx.column_constraint().stream()
                .anyMatch(c -> c.inline_constraint() != null && c.inline_constraint().NOT() != null);
        columnEditor.optional(!hasNotNullConstraint);

        if (ctx.datatype() == null) {
            // Handle case when there's no explicit datatype
            columnEditor.jdbcType(Types.VARCHAR).type("UNKNOWN");
        }
        else {
            resolveColumnDataType(ctx.datatype());
        }
    }

    private void resolveColumnDataType(YashanDbParser.DatatypeContext ctx) {
        // If the context is null, there is nothing this method can resolve and it is safe to return
        if (ctx == null) {
            return;
        }

        if (ctx.native_datatype_element() != null) {
            YashanDbParser.Precision_partContext precisionPart = ctx.precision_part();
            if (ctx.native_datatype_element().INT() != null
                    || ctx.native_datatype_element().INTEGER() != null
                    || ctx.native_datatype_element().PLS_INTEGER() != null) {
                columnEditor
                        .jdbcType(Types.INTEGER)
                        .type("INTEGER");
            }
            else if (ctx.native_datatype_element().SMALLINT() != null) {
                columnEditor
                        .jdbcType(Types.SMALLINT)
                        .type("SMALLINT");
            }
            else if (ctx.native_datatype_element().BIGINT() != null) {
                columnEditor
                        .jdbcType(Types.BIGINT)
                        .type("BIGINT");
            }
            else if (ctx.native_datatype_element().TINYINT() != null) {
                columnEditor
                        .jdbcType(YasTypes.TINYINT)
                        .type("TINYINT");
            }
            else if (ctx.native_datatype_element().BIT() != null) {
                columnEditor
                        .jdbcType(YasTypes.BIT)
                        .type("BIT");
            }
            else if (ctx.native_datatype_element().BOOLEAN() != null) {
                columnEditor
                        .jdbcType(Types.BOOLEAN)
                        .type("BOOLEAN");
            }
            else if (ctx.native_datatype_element().NUMBER() != null) {
                columnEditor
                        .jdbcType(Types.NUMERIC)
                        .type("NUMBER");

                if (precisionPart == null) {
                    columnEditor.length(38);
                }
                else {
                    if (precisionPart.ASTERISK() != null) {
                        // when asterisk is used, explicitly set precision to 38
                        columnEditor.length(38);
                    }
                    else {
                        setPrecision(precisionPart, columnEditor);
                    }
                    setScale(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().FLOAT() != null) {
                columnEditor
                        .jdbcType(Types.FLOAT)
                        .type("FLOAT")
                        .length(126);

                if (precisionPart != null) {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().DOUBLE() != null) {
                columnEditor
                        .jdbcType(Types.DOUBLE)
                        .type("DOUBLE")
                        .length(256);

                if (precisionPart != null) {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().DATE() != null) {
                // JDBC driver reports type as timestamp but name DATE
                columnEditor
                        .jdbcType(Types.TIMESTAMP)
                        .type("DATE");
            }
            else if (ctx.native_datatype_element().TIMESTAMP() != null) {
                columnEditor
                        .jdbcType(Types.TIMESTAMP)
                        .type("TIMESTAMP");

                if (precisionPart == null) {
                    columnEditor.length(6);
                }
                else {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().JSON() != null) {
                columnEditor
                        .jdbcType(YasTypes.JSON)
                        .type("JSON");
            }
            // VARCHAR and VARCHAR2
            else if (ctx.native_datatype_element().VARCHAR2() != null ||
                    ctx.native_datatype_element().VARCHAR() != null) {
                columnEditor
                        .jdbcType(Types.VARCHAR)
                        .type("VARCHAR2");

                if (precisionPart == null) {
                    columnEditor.length(getVarCharDefaultLength());
                }
                else {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            // NCHAR and NVARCHAR
            else if (ctx.native_datatype_element().NVARCHAR() != null) {
                columnEditor
                        .jdbcType(Types.NVARCHAR)
                        .type("NVARCHAR");

                if (precisionPart == null) {
                    columnEditor.length(getVarCharDefaultLength());
                }
                else {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().NCHAR() != null) {
                columnEditor
                        .jdbcType(Types.NCHAR)
                        .type("NCHAR")
                        .length(1);

                if (precisionPart != null) {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().CHAR() != null) {
                columnEditor
                        .jdbcType(Types.CHAR)
                        .type("CHAR")
                        .length(1);

                if (precisionPart != null) {
                    setPrecision(precisionPart, columnEditor);
                }
            }
            else if (ctx.native_datatype_element().BLOB() != null) {
                columnEditor
                        .jdbcType(Types.BLOB)
                        .type("BLOB");
            }
            else if (ctx.native_datatype_element().CLOB() != null) {
                columnEditor
                        .jdbcType(Types.CLOB)
                        .type("CLOB");
            }
            else if (ctx.native_datatype_element().NCLOB() != null) {
                columnEditor
                        .jdbcType(Types.NCLOB)
                        .type("NCLOB");
            }
            else if (ctx.native_datatype_element().RAW() != null) {
                columnEditor
                        .jdbcType(YasTypes.RAW)
                        .type("RAW");

                setPrecision(precisionPart, columnEditor);
            }
            else if (ctx.native_datatype_element().ROWID() != null || ctx.native_datatype_element().UROWID() != null) {
                columnEditor
                        .jdbcType(Types.VARCHAR)
                        .type("ROWID");
            }
            else if (ctx.native_datatype_element().XMLTYPE() != null) {
                columnEditor
                        .jdbcType(YasTypes.SQLXML)
                        .type("XMLTYPE");
            }
            else {
                columnEditor
                        .jdbcType(YasTypes.OTHER)
                        .type(ctx.native_datatype_element().getText());
            }
        }
        else if (ctx.INTERVAL() != null
                && ctx.YEAR() != null
                && ctx.TO() != null
                && ctx.MONTH() != null) {
            columnEditor
                    .jdbcType(YasTypes.YM_INTERVAL)
                    .type("INTERVAL YEAR TO MONTH")
                    .length(2);
            if (!ctx.expression().isEmpty()) {
                columnEditor.length(Integer.valueOf((ctx.expression(0).getText())));
            }
        }
        else if (ctx.INTERVAL() != null
                && ctx.DAY() != null
                && ctx.TO() != null
                && ctx.SECOND() != null) {
            columnEditor
                    .jdbcType(YasTypes.DS_INTERVAL)
                    .type("INTERVAL DAY TO SECOND")
                    .length(2)
                    .scale(6);
            for (final YashanDbParser.ExpressionContext e : ctx.expression()) {
                if (e.getSourceInterval().startsAfter(ctx.TO().getSourceInterval())) {
                    columnEditor.scale(Integer.valueOf(e.getText()));
                }
                else {
                    columnEditor.length(Integer.valueOf(e.getText()));
                }
            }
            if (!ctx.expression().isEmpty()) {
                columnEditor.length(Integer.valueOf((ctx.expression(0).getText())));
            }
        }
        else {
            columnEditor.jdbcType(YasTypes.OTHER).type(ctx.getText());
        }
    }

    private int getVarCharDefaultLength() {
        // TODO replace with value from select name, value from v$parameter where name='max_string_size';
        return 4000;
    }

    private void setPrecision(YashanDbParser.Precision_partContext precisionPart, ColumnEditor columnEditor) {
        if (precisionPart != null && !precisionPart.numeric().isEmpty()) {
            columnEditor.length(Integer.valueOf(precisionPart.numeric(0).getText()));
        }
    }

    private void setScale(YashanDbParser.Precision_partContext precisionPart, ColumnEditor columnEditor) {
        if (precisionPart != null) {
            if (precisionPart.numeric().size() > 1) {
                columnEditor.scale(Integer.valueOf(precisionPart.numeric(1).getText()));
            }
            else if (precisionPart.numeric_negative() != null) {
                columnEditor.scale(Integer.valueOf(precisionPart.numeric_negative().getText()));
            }
            else {
                columnEditor.scale(0);
            }
        }
    }
}