/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yashandb.jdbc.YasTypes;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;

/** Reads Native mode table metadata. */
public final class YashanDbNativeMetadataReader implements YashanDbMetadataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDbNativeMetadataReader.class);
    private final YashanDbNativeDialect dialect = new YashanDbNativeDialect();
    private static final int YASHANDB_UNSET_SCALE = -127;

    @Override
    public List<TableMetadata> readTables(JdbcConnection connection, String catalogName, String schemaName)
            throws SQLException {
        return connection.prepareQueryAndMap(dialect.tableIdsQuery(schemaName), statement -> {
            if (schemaName != null) {
                statement.setString(1, schemaName);
            }
        }, resultSet -> {
            List<TableMetadata> tables = new ArrayList<>();
            while (resultSet.next()) {
                tables.add(new TableMetadata(dialect.tableId(null, resultSet.getString(1), resultSet.getString(2)), "TABLE"));
            }
            return tables;
        });
    }

    @Override
    public Map<TableId, List<String>> readPrimaryKeyOrUniqueIndexNames(JdbcConnection connection, Collection<TableId> tableIds,
                                                                       BiPredicate<String, String> indexFilter)
            throws SQLException {
        Map<TableId, List<String>> keysByTable = new HashMap<>();
        for (Map.Entry<String, List<TableId>> schemaTables : groupBySchema(tableIds).entrySet()) {
            List<TableId> schemaTableIds = schemaTables.getValue();
            String sql = dialect.primaryKeyOrUniqueIndexQuery(schemaTableIds.size());
            Map<TableId, List<String>> schemaKeys = connection.prepareQueryAndMap(sql, statement -> {
                int parameterIndex = 1;
                statement.setString(parameterIndex++, schemaTables.getKey());
                for (TableId tableId : schemaTableIds) {
                    statement.setString(parameterIndex++, tableId.table());
                }
                statement.setString(parameterIndex++, schemaTables.getKey());
                for (TableId tableId : schemaTableIds) {
                    statement.setString(parameterIndex++, tableId.table());
                }
            }, resultSet -> KeyMetadataReader.read(resultSet, schemaTableIds, indexFilter));
            keysByTable.putAll(schemaKeys);
        }
        return keysByTable;
    }

    @Override
    public Map<TableId, List<Column>> readColumns(JdbcConnection connection, Collection<TableId> tableIds, ColumnNameFilter columnFilter,
                                                  UnaryOperator<ColumnEditor> columnOverride, ToIntFunction<String> nativeTypeResolver)
            throws SQLException {
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();
        for (Map.Entry<String, List<TableId>> schemaTables : groupBySchema(tableIds).entrySet()) {
            columnsByTable.putAll(readColumns(connection, schemaTables.getKey(), schemaTables.getValue(),
                    columnFilter, columnOverride, nativeTypeResolver));
        }
        return columnsByTable;
    }

    private Map<TableId, List<Column>> readColumns(JdbcConnection connection, String schemaName, List<TableId> tableIds,
                                                   ColumnNameFilter columnFilter, UnaryOperator<ColumnEditor> columnOverride,
                                                   ToIntFunction<String> nativeTypeResolver)
            throws SQLException {
        Map<String, TableId> tablesByName = new HashMap<>();
        for (TableId tableId : tableIds) {
            tablesByName.put(tableId.table(), tableId);
        }
        return connection.prepareQueryAndMap(dialect.columnsQuery(tableIds.size()), statement -> {
            statement.setString(1, schemaName);
            for (int i = 0; i < tableIds.size(); i++) {
                statement.setString(i + 2, tableIds.get(i).table());
            }
        }, resultSet -> readColumns(resultSet, tablesByName, columnFilter, columnOverride, nativeTypeResolver));
    }

    private static Map<String, List<TableId>> groupBySchema(Collection<TableId> tableIds) {
        Map<String, List<TableId>> tablesBySchema = new LinkedHashMap<>();
        for (TableId tableId : tableIds) {
            tablesBySchema.computeIfAbsent(tableId.schema(), ignored -> new ArrayList<>()).add(tableId);
        }
        return tablesBySchema;
    }

    private Map<TableId, List<Column>> readColumns(ResultSet resultSet, Map<String, TableId> tablesByName,
                                                   ColumnNameFilter columnFilter, UnaryOperator<ColumnEditor> columnOverride,
                                                   ToIntFunction<String> nativeTypeResolver)
            throws SQLException {
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();
        Map<TableId, Set<String>> lowercaseColumnNamesByTable = new HashMap<>();

        while (resultSet.next()) {
            // Read LONG values first, as required by Oracle-compatible drivers.
            String defaultValue = resultSet.getString(10);
            String tableName = resultSet.getString(2);
            TableId tableId = tableName == null ? null : tablesByName.get(tableName);
            if (tableId == null) {
                LOGGER.debug("Ignoring column metadata for table '{}' outside the requested batch", tableName);
                continue;
            }
            String columnName = resultSet.getString(3);
            String lowercaseColumnName = columnName.toLowerCase(Locale.ROOT);
            Set<String> lowercaseColumnNames = lowercaseColumnNamesByTable.computeIfAbsent(tableId, ignored -> new HashSet<>());
            if (!lowercaseColumnNames.add(lowercaseColumnName)) {
                LOGGER.warn("Table '{}' has columns that differ only by case. Column name: '{}'. "
                        + "Debezium does not support case-sensitive duplicate column names.", tableId, columnName);
            }
            if (columnFilter != null && !columnFilter.matches(null, tableId.schema(), tableId.table(), columnName)) {
                continue;
            }

            String typeName = baseTypeName(resultSet.getString(4));
            Integer dataLength = nullableInteger(resultSet, 5);
            Integer charLength = nullableInteger(resultSet, 6);
            Integer precision = nullableInteger(resultSet, 7);
            Integer scale = nullableInteger(resultSet, 8);

            ColumnEditor column = Column.editor()
                    .name(columnName)
                    .type(typeName)
                    .jdbcType(resolveJdbcType(typeName))
                    .nativeType(nativeTypeResolver.applyAsInt(typeName))
                    .length(columnLength(typeName, dataLength, charLength, precision, scale))
                    .optional(!"N".equalsIgnoreCase(resultSet.getString(9)))
                    .position(resultSet.getInt(11));
            if (scale != null && scale != YASHANDB_UNSET_SCALE) {
                column.scale(scale);
            }

            column = columnOverride.apply(column);
            if (defaultValue != null) {
                column.defaultValueExpression(defaultValue);
            }
            columnsByTable.computeIfAbsent(tableId, ignored -> new ArrayList<>()).add(column.create());
        }
        return columnsByTable;
    }

    private static Integer nullableInteger(ResultSet resultSet, int columnIndex) throws SQLException {
        Object value = resultSet.getObject(columnIndex);
        if (value == null) {
            return null;
        }
        return value instanceof Number ? ((Number) value).intValue() : Integer.valueOf(value.toString());
    }

    private static String baseTypeName(String dataType) {
        if (dataType == null || dataType.trim().isEmpty()) {
            return "UNKNOWN";
        }
        StringBuilder name = new StringBuilder();
        int depth = 0;
        for (char character : dataType.trim().toUpperCase(Locale.ROOT).toCharArray()) {
            if (character == '(') {
                depth++;
            }
            else if (character == ')') {
                if (depth > 0) {
                    depth--;
                }
            }
            else if (depth == 0) {
                name.append(character);
            }
        }
        return name.toString().trim().replaceAll("\\s+", " ");
    }

    private static int columnLength(String typeName, Integer dataLength, Integer charLength, Integer precision, Integer scale) {
        switch (typeName) {
            case "NUMBER":
            case "NUMERIC":
            case "DECIMAL":
                return precision != null && precision > 0 ? precision : 38;
            case "BINARY_FLOAT":
            case "FLOAT":
            case "BINARY_DOUBLE":
            case "DOUBLE":
            case "REAL":
                return precision != null && precision > 0 ? precision : valueOrZero(dataLength);
            case "CHAR":
            case "CHARACTER":
            case "NCHAR":
            case "VARCHAR":
            case "VARCHAR2":
            case "CHARACTER VARYING":
            case "NVARCHAR":
            case "NVARCHAR2":
                return charLength != null && charLength > 0 ? charLength : valueOrZero(dataLength);
            case "DATE":
                return scale != null && scale >= 0 ? scale : 0;
            case "TIME":
            case "TIMESTAMP":
            case "TIMESTAMP WITH TIME ZONE":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
                return scale != null && scale >= 0 ? scale : 6;
            case "INTERVAL YEAR TO MONTH":
            case "INTERVAL DAY TO SECOND":
                return precision != null && precision > 0 ? precision : 2;
            default:
                return valueOrZero(dataLength);
        }
    }

    private static int valueOrZero(Integer value) {
        return value == null ? 0 : value;
    }

    /**
     * Resolves the JDBC type of a native-mode column from its {@code ALL_TAB_COLS.DATA_TYPE} name.
     * <p>
     * Only the data types documented for yashan mode in the YashanDB "Data Type Specifications" page
     * (the {@code yashan模式数据类型} table, doc.yashandb.com) are mapped here, together with the
     * aliases that the same documentation defines for them. Names that a native-mode column cannot
     * have, such as the mysql-mode types ({@code TINYINT UNSIGNED}, {@code BINARY},
     * {@code VARBINARY}, ...) or the internal driver type names ({@code SHORTDATE},
     * {@code TIMESTAMP_TZ}, ...), deliberately fall through to {@link Types#OTHER} instead of
     * being guessed. The {@code BINARY_TINYINT}/{@code BINARY_SMALLINT}/{@code BINARY_INTEGER}/
     * {@code BINARY_BIGINT} storage types are left unmapped as well, because the numeric data type
     * chapter does not list them as column data types.
     *
     * @param typeName the base data type name, already upper-cased and stripped of its precision
     * @return the JDBC type code for the data type; {@link Types#OTHER} when it is not a native data type
     */
    private static int resolveJdbcType(String typeName) {
        switch (typeName) {
            // Numeric types
            case "TINYINT":
                return Types.TINYINT;
            case "SMALLINT":
                return Types.SMALLINT;
            case "INT":
            case "INTEGER":
            case "PLS_INTEGER":
                return Types.INTEGER;
            case "BIGINT":
                return Types.BIGINT;
            case "BINARY_FLOAT":
            case "FLOAT":
            case "REAL":
                return Types.REAL;
            case "BINARY_DOUBLE":
            case "DOUBLE":
                return Types.DOUBLE;
            case "NUMBER":
            case "NUMERIC":
            case "DECIMAL":
                return Types.NUMERIC;
            case "BIT":
                return Types.BIT;
            // Boolean type
            case "BOOLEAN":
                return Types.BOOLEAN;
            // Character types
            case "CHAR":
            case "CHARACTER":
                return Types.CHAR;
            case "NCHAR":
                return Types.NCHAR;
            case "VARCHAR":
            case "VARCHAR2":
            case "CHARACTER VARYING":
                return Types.VARCHAR;
            case "NVARCHAR":
            case "NVARCHAR2":
                return Types.NVARCHAR;
            // Date-time types
            case "DATE":
                return Types.TIMESTAMP;
            case "TIME":
                return Types.TIME;
            case "TIMESTAMP":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
                return Types.TIMESTAMP;
            case "TIMESTAMP WITH TIME ZONE":
                return YasTypes.TIMESTAMP_TZ;
            case "INTERVAL YEAR TO MONTH":
                return YasTypes.YM_INTERVAL;
            case "INTERVAL DAY TO SECOND":
                return YasTypes.DS_INTERVAL;
            // Large object types
            case "BLOB":
            case "BFILE":
                return Types.BLOB;
            case "CLOB":
                return Types.CLOB;
            case "NCLOB":
                return Types.NCLOB;
            case "XMLTYPE":
                return YasTypes.SQLXML;
            // ROWID, RAW and JSON types
            case "RAW":
            case "UROWID":
                // UROWID is handled as RAW internally, so it is exchanged as binary data.
                return YasTypes.RAW;
            case "ROWID":
                return YasTypes.ROWID;
            case "JSON":
                return YasTypes.JSON;
            // Spatial types
            case "ST_GEOMETRY":
            case "BOX2D":
                return Types.BLOB;
            // Vector type
            case "VECTOR":
                return Types.OTHER;
            // User-defined object, collection and reference types
            case "OBJECT":
                return Types.STRUCT;
            case "REF":
                return Types.REF;
            case "ARRAY":
                return Types.ARRAY;
            default:
                return Types.OTHER;
        }
    }
}
