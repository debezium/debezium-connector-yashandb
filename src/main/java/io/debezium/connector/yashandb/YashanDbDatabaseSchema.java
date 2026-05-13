/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yashandb.jdbc.YasTypes;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.yashandb.antlr.YashanDbDdlParser;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * The schema of an YashanDB database.
 */
public class YashanDbDatabaseSchema extends HistorizedRelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDbDatabaseSchema.class);
    private final YashanDbDdlParser ddlParser;
    private final ConcurrentMap<TableId, List<Column>> lobColumnsByTableId = new ConcurrentHashMap<>();
    private final YashanDbValueConverters valueConverters;

    private boolean storageInitializationExecuted = false;

    /**
     * Creates a new database schema instance.
     *
     * @param connectorConfig the connector configuration
     * @param valueConverters the value converters
     * @param defaultValueConverter the default value converter
     * @param schemaNameAdjuster the schema name adjuster
     * @param topicNamingStrategy the topic naming strategy
     * @param tableNameCaseSensitivity the table name case sensitivity
     * @param customConverterRegistry the custom converter registry
     * @param taskContext the task context
     */
    public YashanDbDatabaseSchema(YashanDbConnectorConfig connectorConfig, YashanDbValueConverters valueConverters,
                                  DefaultValueConverter defaultValueConverter, SchemaNameAdjuster schemaNameAdjuster,
                                  TopicNamingStrategy<TableId> topicNamingStrategy,
                                  StreamingAdapter.TableNameCaseSensitivity tableNameCaseSensitivity,
                                  CustomConverterRegistry customConverterRegistry,
                                  CdcSourceTaskContext<? extends CommonConnectorConfig> taskContext) {
        super(connectorConfig, topicNamingStrategy, connectorConfig.getTableFilters().dataCollectionFilter(),
                connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverters,
                        defaultValueConverter,
                        schemaNameAdjuster,
                        customConverterRegistry,
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getFieldNamer(),
                        false,
                        connectorConfig.getEventConvertingFailureHandlingMode()),
                StreamingAdapter.TableNameCaseSensitivity.INSENSITIVE.equals(tableNameCaseSensitivity),
                connectorConfig.getKeyMapper(), taskContext);

        this.valueConverters = valueConverters;
        this.ddlParser = new YashanDbDdlParser(
                true,
                false,
                connectorConfig.isSchemaCommentsHistoryEnabled(),
                valueConverters,
                connectorConfig.getTableFilters().dataCollectionFilter());
    }

    /**
     * Returns the tables tracked by this schema.
     *
     * @return the tables
     */
    public Tables getTables() {
        return tables();
    }

    /**
     * Returns the value converters used by this schema.
     *
     * @return the value converters
     */
    public YashanDbValueConverters getValueConverters() {
        return valueConverters;
    }

    @Override
    public YashanDbDdlParser getDdlParser() {
        return ddlParser;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        LOGGER.debug("Applying schema change event {}", schemaChange);

        switch (schemaChange.getType()) {
            case CREATE:
            case ALTER:
                schemaChange.getTableChanges().forEach(x -> {
                    buildAndRegisterSchema(x.getTable());
                    tables().overwriteTable(x.getTable());
                });
                break;
            case DROP:
                schemaChange.getTableChanges().forEach(x -> removeSchema(x.getId()));
                break;
            default:
        }

        if (!storeOnlyCapturedTables() ||
                schemaChange.getTables().stream().map(Table::id).anyMatch(getTableFilter()::isIncluded)) {
            LOGGER.debug("Recorded DDL statements for database '{}': {}", schemaChange.getDatabase(), schemaChange.getDdl());
            record(schemaChange, schemaChange.getTableChanges());
        }
    }

    @Override
    public void initializeStorage() {
        super.initializeStorage();
        storageInitializationExecuted = true;
    }

    /**
     * Returns whether storage initialization has been executed.
     *
     * @return true if storage was initialized, false otherwise
     */
    public boolean isStorageInitializationExecuted() {
        return storageInitializationExecuted;
    }

    /**
     * Return true if the database schema history entity exists
     */
    /**
     * Returns whether the database schema history exists.
     *
     * @return true if history exists, false otherwise
     */
    public boolean historyExists() {
        return schemaHistory.exists();
    }

    @Override
    protected void removeSchema(TableId id) {
        super.removeSchema(id);
        lobColumnsByTableId.remove(id);
    }

    @Override
    protected void buildAndRegisterSchema(Table table) {
        if (getTableFilter().isIncluded(table.id())) {
            super.buildAndRegisterSchema(table);

            // Cache LOB column mappings for performance
            buildAndRegisterTableLobColumns(table);
        }
    }

    /**
     * Get a list of large object (LOB) columns for the specified relational table identifier.
     *
     * @param id the relational table identifier
     * @return a list of LOB columns, may be empty if the table has no LOB columns
     */
    public List<Column> getLobColumnsForTable(TableId id) {
        return lobColumnsByTableId.getOrDefault(id, Collections.emptyList());
    }

    /**
     * Returns whether the specified value is the unavailable value placeholder for an LOB column.
     */
    public boolean isColumnUnavailableValuePlaceholder(Column column, Object value) {
        if (isClobColumn(column) || isXmlColumn(column)) {
            return valueConverters.getUnavailableValuePlaceholderString().equals(value);
        }
        else if (isBlobColumn(column)) {
            return ByteBuffer.wrap(valueConverters.getUnavailableValuePlaceholderBinary()).equals(value);
        }
        return false;
    }

    /**
     * Return whether the provided relational column model is a LOB data type.
     */
    public static boolean isLobColumn(Column column) {
        return isClobColumn(column) || isBlobColumn(column);
    }

    /**
     * Return whether the provided relational column model is a XML data type.
     */
    public static boolean isXmlColumn(Column column) {
        return column.jdbcType() == YasTypes.SQLXML;
    }

    /**
     * Returns whether the provided relational column model is a CLOB or NCLOB data type.
     */
    private static boolean isClobColumn(Column column) {
        return column.jdbcType() == YasTypes.CLOB || column.jdbcType() == YasTypes.NCLOB;
    }

    /**
     * Returns whether the provided relational column model is a CLOB data type.
     */
    private static boolean isBlobColumn(Column column) {
        return column.jdbcType() == YasTypes.BLOB;
    }

    private void buildAndRegisterTableLobColumns(Table table) {
        final List<Column> lobColumns = new ArrayList<>();
        for (Column column : table.columns()) {
            switch (column.jdbcType()) {
                case YasTypes.CLOB:
                case YasTypes.NCLOB:
                case YasTypes.BLOB:
                case YasTypes.JSON:
                case YasTypes.SQLXML:
                    lobColumns.add(column);
                    break;
            }
        }
        if (!lobColumns.isEmpty()) {
            lobColumnsByTableId.put(table.id(), lobColumns);
        }
        else {
            lobColumnsByTableId.remove(table.id());
        }
    }
}
