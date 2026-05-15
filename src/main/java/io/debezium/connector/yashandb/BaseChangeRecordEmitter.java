/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope.Operation;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

/**
 * Base class to emit change data based on a single entry event.
 */
public abstract class BaseChangeRecordEmitter<T> extends RelationalChangeRecordEmitter<YashanDbPartition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseChangeRecordEmitter.class);

    private final YashanDbConnectorConfig connectorConfig;
    private final Object[] oldColumnValues;
    private final Object[] newColumnValues;
    private final YashanDbDatabaseSchema schema;
    protected final Table table;

    /**
     * Creates a BaseChangeRecordEmitter instance initialized with the given parameters for emitting change data based on a single entry event.
     *
     * @param connectorConfig the connector configuration
     *
     * @param partition the source partition
     *
     * @param offset the offset context
     *
     * @param schema the database schema
     *
     * @param table the relational table
     *
     * @param clock the clock for time-based operations
     *
     * @param oldColumnValues the old column values before the change
     *
     * @param newColumnValues the new column values after the change
     */
    protected BaseChangeRecordEmitter(YashanDbConnectorConfig connectorConfig, YashanDbPartition partition, OffsetContext offset,
                                      YashanDbDatabaseSchema schema, Table table, Clock clock, Object[] oldColumnValues,
                                      Object[] newColumnValues) {
        super(partition, offset, clock, connectorConfig);
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.oldColumnValues = oldColumnValues;
        this.newColumnValues = newColumnValues;
        this.table = table;
    }

    /**
     * Returns the old column values for the change event.
     *
     * @return the array of old column values
     */
    @Override
    protected Object[] getOldColumnValues() {
        return oldColumnValues;
    }

    /**
     * Returns the new column values for the change event.
     *
     * @return the array of new column values
     */
    @Override
    protected Object[] getNewColumnValues() {
        return newColumnValues;
    }

    @Override
    protected void emitTruncateRecord(Receiver receiver, TableSchema tableSchema) throws InterruptedException {
        Struct envelope = tableSchema.getEnvelopeSchema().truncate(getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Operation.TRUNCATE, null, envelope, getOffset(), null);
    }

    @Override
    protected void emitUpdateRecord(Receiver<YashanDbPartition> receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Object[] newColumnValues = getNewColumnValues();

        Struct oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Struct newKey = tableSchema.keyFromColumnData(newColumnValues);

        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
            LOGGER.debug("no new values found for table '{}' from update message at '{}'; skipping record", tableSchema, getOffset().getSourceInfo());
            return;
        }

        /*
         * If skip.messages.without.change is configured true,
         * Skip Publishing the message in case there is no change in monitored columns
         * (Postgres) Only works if REPLICA IDENTITY is set to FULL - as oldValues won't be available
         */
        if (skipMessagesWithoutChange() && Objects.nonNull(newValue) && newValue.equals(oldValue)) {
            LOGGER.debug("No new values found for table '{}' in included columns from update message at '{}'; skipping record", tableSchema,
                    getOffset().getSourceInfo());
            return;
        }
        // some configurations does not provide old values in case of updates
        // in this case we handle all updates as regular ones
        if (oldKey == null || Objects.equals(oldKey, newKey)) {
            newValue = notUpdateLobNeedQueryLob(tableSchema, newKey, newValue);
            Struct envelope = tableSchema.getEnvelopeSchema().update(oldValue, newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
            receiver.changeRecord(getPartition(), tableSchema, Operation.UPDATE, newKey, envelope, getOffset(), null);
        }
        // PK update -> emit as delete and re-insert with new key
        else {
            emitUpdateAsPrimaryKeyChangeRecord(receiver, tableSchema, oldKey, newKey, oldValue, newValue);
        }
    }

    private Struct notUpdateLobNeedQueryLob(TableSchema tableSchema, Struct newKey, Struct newValue) {
        final List<Column> reselectColumns = getReselectColumns(newValue);
        if (!reselectColumns.isEmpty()) {
            LOGGER.info("Table '{}' primary key '{}' via an UPDATE, re-selecting LOB columns {} out of bands.",
                    table.id(), newKey, reselectColumns.stream().map(Column::name).collect(Collectors.toList()));

            final JdbcConfiguration jdbcConfig = connectorConfig.getJdbcConfig();
            try (YashanDbConnection connection = new YashanDbConnection(jdbcConfig)) {
                final String query = getReselectQuery(reselectColumns, table, connection);
                connection.prepareQuery(query,
                        ps -> prepareReselectQueryStatement(ps, table, newKey),
                        rs -> updateNewValuesFromReselectQueryResults(rs, reselectColumns));

                // newColumnValues have been updated via re-select, re-create the event's value
                //
                // NOTE: The conversion of the column data must occur within the current connection's context.
                // This is because the converters for LOB may make specific callbacks to the underlying database
                // and if the converter is called outside the scope of the current LOB object, the call will
                // fail due to database connection being unavailable.
                newValue = tableSchema.valueFromColumnData(newColumnValues);
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed to re-select table with LOB columns due to primary key update", e);
            }
        }
        return newValue;
    }

    @Override
    protected void emitUpdateAsPrimaryKeyChangeRecord(Receiver<YashanDbPartition> receiver, TableSchema tableSchema, Struct oldKey,
                                                      Struct newKey, Struct oldValue, Struct newValue)
            throws InterruptedException {
        if (connectorConfig.isLobEnabled()) {
            final List<Column> reselectColumns = getReselectColumns(newValue);
            if (!reselectColumns.isEmpty()) {
                LOGGER.info("Table '{}' primary key changed from '{}' to '{}' via an UPDATE, re-selecting LOB columns {} out of bands.",
                        table.id(), oldKey, newKey, reselectColumns.stream().map(Column::name).collect(Collectors.toList()));

                final JdbcConfiguration jdbcConfig = connectorConfig.getJdbcConfig();
                try (YashanDbConnection connection = new YashanDbConnection(jdbcConfig)) {
                    final String query = getReselectQuery(reselectColumns, table, connection);
                    connection.prepareQuery(query,
                            ps -> prepareReselectQueryStatement(ps, table, newKey),
                            rs -> updateNewValuesFromReselectQueryResults(rs, reselectColumns));

                    // newColumnValues have been updated via re-select, re-create the event's value
                    //
                    // NOTE: The conversion of the column data must occur within the current connection's context.
                    // This is because the converters for LOB may make specific callbacks to the underlying database
                    // and if the converter is called outside the scope of the current LOB object, the call will
                    // fail due to database connection being unavailable.
                    newValue = tableSchema.valueFromColumnData(newColumnValues);
                }
                catch (SQLException e) {
                    throw new DebeziumException("Failed to re-select table with LOB columns due to primary key update", e);
                }
            }
        }
        super.emitUpdateAsPrimaryKeyChangeRecord(receiver, tableSchema, oldKey, newKey, oldValue, newValue);
    }

    /**
     * Returns a list of columns that should be reselected.
     *
     * Currently, this method is only concerned about LOB-based columns and so if a table does not have any
     * LOB columns or if the LOB column's value is not the unavailable value placeholder configured in the
     * connector configuration, this method may return no columns indicating that a reselection is not
     * required for the change event.
     *
     * @param newValue the currently constructed new value payload for the change event, should not be null
     * @return list of columns that should be reselected, which can be empty
     */
    private List<Column> getReselectColumns(Struct newValue) {
        final List<Column> reselectColumns = new ArrayList<>();
        for (Column column : schema.getLobColumnsForTable(table.id())) {
            final Object value = newValue.get(column.name());
            if (schema.isColumnUnavailableValuePlaceholder(column, value)) {
                reselectColumns.add(column);
            }
        }
        return reselectColumns;
    }

    /**
     * Creates the reselect query, a query that explicitly only selects the LOB-based columns from the
     * underlying relational table based on the event's current primary key value set.
     *
     * @param reselectColumns the columns that should be reselected, should never be null or empty
     * @param table the relational table model
     * @param connection the database connection
     * @return the query string for the reselect query
     */
    private String getReselectQuery(List<Column> reselectColumns, Table table, YashanDbConnection connection) {
        final TableId id = new TableId(null, table.id().schema(), table.id().table());
        final StringBuilder query = new StringBuilder("SELECT ")
                .append(reselectColumns.stream().map(c -> connection.quoteIdentifier(c.name())).collect(Collectors.joining(", ")))
                .append(" FROM ")
                .append(id.toDoubleQuotedString())
                .append(" WHERE ");

        for (int i = 0; i < table.primaryKeyColumnNames().size(); ++i) {
            if (i > 0) {
                query.append(" AND ");
            }
            query.append(connection.quoteIdentifier(table.primaryKeyColumnNames().get(i))).append("=?");
        }

        return query.toString();
    }

    /**
     * Prepares the reselect query, binding the primary key column values
     *
     * @param ps the prepared statement
     * @param table the relational model table
     * @param newKey the row's new key
     * @throws SQLException if a database error occurred
     */
    private void prepareReselectQueryStatement(PreparedStatement ps, Table table, Struct newKey) throws SQLException {
        for (int i = 0; i < table.primaryKeyColumnNames().size(); ++i) {
            ps.setObject(i + 1, newKey.get(table.primaryKeyColumnNames().get(i)));
        }
    }

    /**
     * Applies the reselect query results to the new column values object array.
     *
     * @param rs the reselect query result set
     * @param reselectColumns the columns that were re-selected
     * @throws SQLException if a database error occurred
     */
    private void updateNewValuesFromReselectQueryResults(ResultSet rs, List<Column> reselectColumns) throws SQLException {
        if (rs.next()) {
            for (int i = 0; i < reselectColumns.size(); ++i) {
                final Column column = reselectColumns.get(i);
                newColumnValues[column.position() - 1] = rs.getObject(i + 1);
            }
        }
    }
}
