/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sics.ystream.exception.YstreamSqlException;
import com.sics.ystream.result.YstreamColumn;
import com.sics.ystream.result.YstreamColumns;

import io.debezium.connector.yashandb.BaseChangeRecordEmitter;
import io.debezium.connector.yashandb.YashanDBConnectorConfig;
import io.debezium.connector.yashandb.YashanDBDatabaseSchema;
import io.debezium.connector.yashandb.YashanDBPartition;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

/**
 * Emits change data based on a single {@link YStreamDataChangeRecord} event.
 *
 * @author Gunnar Morling
 */
public class YStreamChangeRecordEmitter extends BaseChangeRecordEmitter<YStreamDataChangeRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YStreamChangeRecordEmitter.class);

    private final YStreamDataChangeRecord record;

    public YStreamChangeRecordEmitter(YashanDBConnectorConfig connectorConfig, YashanDBPartition partition, OffsetContext offset, YStreamDataChangeRecord record,
                                      Table table, YashanDBDatabaseSchema schema, Clock clock, Object[] newValues, Object[] oldValues) {
        super(connectorConfig, partition, offset, schema, table, clock, oldValues,
                newValues);
        this.record = record;
    }

    @Override
    public Operation getOperation() {
        if (record.isTruncateTable()) {
            return Operation.TRUNCATE;
        }
        else {
            switch (record.getYstreamDml().getDmlType()) {
                case INSERT:
                    return Operation.CREATE;
                case DELETE:
                    return Operation.DELETE;
                case UPDATE:
                    return Operation.UPDATE;
                case CHUNK:
                default:
                    throw new IllegalArgumentException("Received event of unexpected command type: " + record);
            }
        }
    }

    /*
     * public static void calculateColumnValues(Object[] oldValues, Object[] newValues) {
     * // calculate values
     * for (int i = 0; i < oldValues.length; i++) {
     * if (oldValues[i] != null && newValues[i] == null) {
     * newValues[i] = oldValues[i];
     * }
     * }
     * }
     */
    /**
     * For UPDATE: YStream sends only changed columns in after (new values). Unchanged columns are absent (null in array).
     * Columns explicitly set to NULL are present in after with value null. We must only fill from old when the column
     * is absent from after; otherwise "update to NULL" would wrongly show the old value.
     *
     * @param oldValues                  before state (full row except non-modified LOB)
     * @param newValues                  after state (only updated columns have values; rest are null)
     * @param columnNamesPresentInAfter  set of column names that appear in the after payload (getNewValues().getColumns())
     * @param table                      table metadata to map index to column name
     */
    public static void calculateColumnValues(Object[] oldValues, Object[] newValues,
                                             Set<String> columnNamesPresentInAfter, Table table) {
        if (columnNamesPresentInAfter == null || table == null) {
            for (int i = 0; i < oldValues.length; i++) {
                if (oldValues[i] != null && newValues[i] == null) {
                    newValues[i] = oldValues[i];
                }
            }
            return;
        }
        for (Column column : table.columns()) {
            int index = column.position() - 1;
            if (index < 0 || index >= oldValues.length) {
                continue;
            }
            // 针对被修改的字段，如果原有的值不为null，修改之后的值为null的时候，新的值不在赋值为原有的值。
            if (columnNamesPresentInAfter.contains(column.name())) {
                continue;
            }
            if (oldValues[index] != null && newValues[index] == null) {
                newValues[index] = oldValues[index];
            }
        }
    }

    public static Object[] getColumnValues(Table table, YstreamColumns columnValues, Map<String, Object> chunkValues) {
        Object[] values = new Object[table.columns().size()];
        if (columnValues != null) {
            List<YstreamColumn> columns = columnValues.getColumns().stream()
                    .filter(ystreamColumn -> !ystreamColumn.getColumn().isDeleted()).collect(Collectors.toList());
            for (YstreamColumn columnValue : columns) {
                try {
                    Column column = table.columnWithName(columnValue.getColumn().getColumnName());
                    if (column != null) {
                        int index = column.position() - 1;
                        values[index] = columnValue.getData();
                    }
                    else {
                        throw new IllegalStateException("The schema metadata is different from event,maybe NPE");
                    }
                }
                catch (YstreamSqlException e) {
                    LOGGER.error("convert data error", e);
                    throw new RuntimeException("convert data error", e);
                }
            }
        }

        // Overlay chunk values into non-chunk value array
        for (Map.Entry<String, Object> entry : chunkValues.entrySet()) {
            final int index = table.columnWithName(entry.getKey()).position() - 1;
            if (values[index] == null) {
                values[index] = entry.getValue();
            }
        }

        return values;
    }
}
