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
import java.util.Collections;
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

/** Reads MySql mode table metadata. */
public final class YashanDbMySqlMetadataReader implements YashanDbMetadataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDbMySqlMetadataReader.class);
    private final YashanDbMySqlDialect dialect = new YashanDbMySqlDialect();

    @Override
    public List<TableMetadata> readTables(JdbcConnection connection, String catalogName, String schemaName)
            throws SQLException {
        String selectedSchema = schemaName == null ? catalogName : schemaName;
        return connection.prepareQueryAndMap(dialect.tableIdsQuery(selectedSchema), statement -> {
            if (selectedSchema != null) {
                statement.setString(1, selectedSchema);
            }
        }, resultSet -> {
            List<TableMetadata> tables = new ArrayList<>();
            while (resultSet.next()) {
                String tableSchema = resultSet.getString(1);
                if (tableSchema == null || tableSchema.isEmpty()) {
                    tableSchema = selectedSchema;
                }
                tables.add(new TableMetadata(dialect.tableId(null, tableSchema, resultSet.getString(2)), "TABLE"));
            }
            return tables;
        });
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

    @Override
    public Map<TableId, List<String>> readPrimaryKeyOrUniqueIndexNames(JdbcConnection connection, Collection<TableId> tableIds,
                                                                       BiPredicate<String, String> indexFilter)
            throws SQLException {
        Map<TableId, List<String>> keysByTable = new HashMap<>();
        for (Map.Entry<String, List<TableId>> schemaTables : groupBySchema(tableIds).entrySet()) {
            List<TableId> schemaTableIds = schemaTables.getValue();
            String sql = dialect.primaryKeyOrUniqueIndexQuery(schemaTableIds.size());
            Map<TableId, List<String>> schemaKeys = connection.prepareQueryAndMap(sql, statement -> {
                statement.setString(1, schemaTables.getKey());
                for (int i = 0; i < schemaTableIds.size(); i++) {
                    statement.setString(i + 2, schemaTableIds.get(i).table());
                }
            }, resultSet -> KeyMetadataReader.read(resultSet, schemaTableIds, indexFilter));
            keysByTable.putAll(schemaKeys);
        }
        return keysByTable;
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

            String columnType = resultSet.getString(4);
            if (columnType == null) {
                columnType = "UNKNOWN";
            }
            String baseType = mySqlBaseType(columnType);

            Integer characterMaximumLength = nullableInteger(resultSet, 5);
            Integer numericPrecision = nullableInteger(resultSet, 6);
            Integer numericScale = nullableInteger(resultSet, 7);
            Integer length = numericPrecision != null ? numericPrecision : characterMaximumLength;
            String defaultValue = resultSet.getString(9);
            String extra = resultSet.getString(11);

            ColumnEditor column = Column.editor()
                    .name(columnName)
                    .type(columnType)
                    .jdbcType(resolveMySqlJdbcType(baseType))
                    .nativeType(nativeTypeResolver.applyAsInt(baseType))
                    .length(length == null ? 0 : length)
                    .optional(isMySqlColumnNullable(resultSet.getString(8)))
                    .position(resultSet.getInt(10))
                    .autoIncremented(hasExtraFlag(extra, "auto_increment"))
                    .generated(hasExtraFlag(extra, "generated"));
            if (numericScale != null) {
                column.scale(numericScale);
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

    private static boolean isMySqlColumnNullable(String nullable) {
        return nullable == null || "YES".equalsIgnoreCase(nullable) || "1".equals(nullable);
    }

    private static boolean hasExtraFlag(String extra, String flag) {
        return extra != null && extra.toLowerCase(Locale.ROOT).contains(flag);
    }

    private static String mySqlBaseType(String columnType) {
        return columnType.replaceFirst("\\s*\\(.*", "")
                .replaceFirst("(?i)\\s+(unsigned|zerofill).*", "")
                .trim();
    }

    private static int resolveMySqlJdbcType(String typeName) {
        switch (typeName.toLowerCase(Locale.ROOT)) {
            case "bit":
                return Types.BIT;
            case "tinyint":
                return Types.TINYINT;
            case "smallint":
                return Types.SMALLINT;
            case "mediumint":
            case "middleint":
            case "int":
            case "integer":
                return Types.INTEGER;
            case "bigint":
                return Types.BIGINT;
            case "decimal":
            case "number":
            case "numeric":
                return Types.NUMERIC;
            case "float":
                return Types.FLOAT;
            case "double":
            case "real":
                return Types.DOUBLE;
            case "bool":
            case "boolean":
                return Types.BOOLEAN;
            case "date":
                return Types.DATE;
            case "time":
                return Types.TIME;
            case "datetime":
            case "timestamp":
                return Types.TIMESTAMP;
            case "timestamp_ltz":
            case "timestamp with local time zone":
                return YasTypes.TIMESTAMP_LTZ;
            case "timestamp_tz":
            case "timestamp with time zone":
                return YasTypes.TIMESTAMP_TZ;
            case "char":
                return Types.CHAR;
            case "nchar":
                return Types.NCHAR;
            case "varchar":
            case "enum":
            case "set":
                return Types.VARCHAR;
            case "nvarchar":
                return Types.NVARCHAR;
            case "binary":
                return Types.BINARY;
            case "varbinary":
                return Types.VARBINARY;
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
                return Types.BLOB;
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
            case "clob":
                return Types.CLOB;
            case "nclob":
                return Types.NCLOB;
            case "json":
                return YasTypes.JSON;
            default:
                return Types.OTHER;
        }
    }

    @Override
    public List<String> readPrimaryKeyNames(JdbcConnection connection, TableId tableId, JdbcKeyMetadataReader jdbcReader) throws SQLException {
        return connection.prepareQueryAndMap(dialect.primaryKeyQuery(), statement -> {
            statement.setString(1, tableId.schema());
            statement.setString(2, tableId.table());
        }, resultSet -> {
            List<String> columnNames = new ArrayList<>();
            while (resultSet.next()) {
                columnNames.add(resultSet.getString(1));
            }
            return columnNames;
        });
    }

    @Override
    public List<String> readTableUniqueIndices(JdbcConnection connection, TableId tableId, BiPredicate<String, String> indexFilter,
                                               JdbcKeyMetadataReader jdbcReader)
            throws SQLException {
        return connection.prepareQueryAndMap(dialect.uniqueIndexQuery(), statement -> {
            statement.setString(1, tableId.schema());
            statement.setString(2, tableId.table());
        }, resultSet -> readUniqueIndexColumns(resultSet, indexFilter));
    }

    private List<String> readUniqueIndexColumns(ResultSet resultSet, BiPredicate<String, String> indexFilter) throws SQLException {
        List<String> columnNames = new ArrayList<>();
        String currentIndexName = null;
        boolean currentIndexIncluded = true;

        while (resultSet.next()) {
            String indexName = resultSet.getString(1);
            String columnName = resultSet.getString(2);
            int columnPosition = resultSet.getInt(3);
            if (indexName == null) {
                continue;
            }

            if (currentIndexName != null && !currentIndexName.equals(indexName)) {
                if (currentIndexIncluded) {
                    return columnNames;
                }
                columnNames.clear();
                currentIndexIncluded = true;
            }
            currentIndexName = indexName;

            if (!indexFilter.test(indexName, columnName)) {
                currentIndexIncluded = false;
                continue;
            }
            if (columnName != null && columnPosition > 0) {
                while (columnNames.size() < columnPosition) {
                    columnNames.add(null);
                }
                columnNames.set(columnPosition - 1, columnName);
            }
        }

        return currentIndexIncluded ? columnNames : Collections.emptyList();
    }
}
