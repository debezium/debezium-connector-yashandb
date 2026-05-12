/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import com.sics.ystream.result.YstreamChunk;
import com.sics.ystream.result.YstreamColumn;
import com.sics.ystream.result.YstreamColumns;
import com.sics.ystream.result.YstreamDdl;
import com.sics.ystream.result.YstreamDml;
import com.sics.ystream.result.YstreamLcrInterface;
import com.sics.ystream.result.YstreamMetadata;
import com.sics.ystream.result.YstreamXactBegin;
import com.sics.ystream.result.YstreamXactCommit;
import com.yashandb.jdbc.YasTypes;
import io.debezium.DebeziumException;
import io.debezium.connector.yashandb.YashanDBConnection;
import io.debezium.connector.yashandb.YashanDBConnectorConfig;
import io.debezium.connector.yashandb.YashanDBDatabaseSchema;
import io.debezium.connector.yashandb.YashanDBOffsetContext;
import io.debezium.connector.yashandb.YashanDBPartition;
import io.debezium.connector.yashandb.YashanDBSchemaChangeEventEmitter;
import io.debezium.connector.yashandb.YashanDBStreamingChangeEventSourceMetrics;
import io.debezium.connector.yashandb.YashanDBValueConverters;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handler for YashanDB DDL and DML events. Just forwards events to the {@link EventDispatcher}.
 *
 * @author Gunnar Morling
 */
class YStreamEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(YStreamEventHandler.class);
    private YStreamDeserializer ystreamDeserializer;
    private final YashanDBConnectorConfig connectorConfig;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<YashanDBPartition, TableId> dispatcher;
    private final Clock clock;
    private final YashanDBDatabaseSchema schema;
    private final YashanDBPartition partition;
    private final YashanDBOffsetContext offsetContext;
    private final YStreamStreamingChangeEventSource eventSource;
    private final YashanDBStreamingChangeEventSourceMetrics streamingMetrics;
    private final Map<String, ChunkColumnValues> columnChunks;
    private YStreamRecord currentRecord;
    private int outRowSize;

    YStreamEventHandler(YashanDBConnectorConfig connectorConfig, ErrorHandler errorHandler,
                        EventDispatcher<YashanDBPartition, TableId> dispatcher, Clock clock,
                        YashanDBDatabaseSchema schema, YashanDBPartition partition, YashanDBOffsetContext offsetContext, YStreamStreamingChangeEventSource eventSource,
                        YashanDBStreamingChangeEventSourceMetrics streamingMetrics) {
        this.connectorConfig = connectorConfig;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.eventSource = eventSource;
        this.streamingMetrics = streamingMetrics;
        this.columnChunks = new LinkedHashMap<>();
    }

    public void processRecord(YStreamRecord record) {
        LOGGER.trace("Received Record {}", record);
        try {
            // First set watermark to flush messages seen
            setWatermark();
            if (!(record.getYstreamLcrInterface() instanceof YstreamChunk)) {
                columnChunks.clear();
            }
            if (!(record.getYstreamLcrInterface() instanceof YstreamMetadata || record.getYstreamLcrInterface() instanceof YstreamXactBegin
                    || record.getYstreamLcrInterface() instanceof YstreamChunk)) {
                final YStreamPosition yStreamPosition = new YStreamPosition(record.getYstreamLcrInterface().getPosition());

                // After a restart it may happen we get the event with the last processed LCR again
                YStreamPosition offsetLcrPosition = new YStreamPosition(offsetContext.getLcrPosition());
                if (yStreamPosition.compareTo(offsetLcrPosition) < 0) {
                    if (LOGGER.isDebugEnabled()) {
                        final YStreamPosition recPosition = offsetLcrPosition;
                        LOGGER.debug("Ignoring change event with already processed SCN/LCR Position {}/{}, last recorded {}/{}",
                                yStreamPosition,
                                yStreamPosition.getRawPosition().getCommitScn().getScn(),
                                recPosition,
                                recPosition.getRawPosition().getCommitScn().getScn());
                    }
                    return;
                }
                offsetContext.setScn(yStreamPosition.getScn());
                offsetContext.setEventScn(yStreamPosition.getScn());
                offsetContext.setLcrPosition(yStreamPosition.getRawPosition());
                offsetContext.setTransactionId(String.valueOf(record.getYstreamLcrInterface().getTransactionId()));
            }
            switch (record.getYstreamLcrInterface().getLcrType()) {
                case YSTREAM_DDL:
                    YstreamDdl ddl = (YstreamDdl) record.getYstreamLcrInterface();
                    offsetContext.tableEvent(new TableId("", ddl.getTableId().getSchema(), ddl.getTableId().getTable()),
                            ddl.getCurrentScn().getTimestamp().toInstant());
                    break;
                case YSTREAM_DML:
                    YstreamDml dml = (YstreamDml) record.getYstreamLcrInterface();
                    offsetContext.tableEvent(new TableId("", dml.getTableId().getSchema(), dml.getTableId().getTable()),
                            dml.getCurrentScn().getTimestamp().toInstant());
                    break;
                case YSTREAM_XACT:
                    break;
                case YSTREAM_CHUNK:
                    YstreamChunk chunk = (YstreamChunk) record.getYstreamLcrInterface();
                    offsetContext.tableEvent(new TableId("", chunk.getTableId().getSchema(), chunk.getTableId().getTable()),
                            chunk.getCurrentScn().getTimestamp().toInstant());
                    break;
                case YSTREAM_METADATA:
                    YstreamMetadata metadata = (YstreamMetadata) record.getYstreamLcrInterface();
                    offsetContext.tableEvent(new TableId("", metadata.getTableId().getSchema(), metadata.getTableId().getTable()),
                            null);
            }
            YstreamLcrInterface ystreamLcrInterface = record.getYstreamLcrInterface();
            if (record.getYstreamLcrInterface() instanceof YstreamDdl) {
                dispatchSchemaChangeEvent((YstreamDdl) ystreamLcrInterface);
            } else if (ystreamLcrInterface instanceof YstreamDml) {
                processDml(new YStreamDataChangeRecord((YstreamDml) ystreamLcrInterface, record.getTableMetadata()), (YstreamDml) ystreamLcrInterface);
            } else if (ystreamLcrInterface instanceof YstreamChunk) {
                processChunk((YstreamChunk) ystreamLcrInterface);
            } else if (ystreamLcrInterface instanceof YstreamXactBegin) {
                dispatcher.dispatchTransactionStartedEvent(partition, String.valueOf(ystreamLcrInterface.getTransactionId()), offsetContext,
                        ((YstreamXactBegin) ystreamLcrInterface).getCommitScn().getTimestamp().toInstant());
            } else if (ystreamLcrInterface instanceof YstreamXactCommit) {
                dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext,
                        ystreamLcrInterface.getPosition().getCommitScn().getTimestamp().toInstant());
            }
        }
        // nothing to be done here if interrupted; the event loop will be stopped in the streaming source
        catch (InterruptedException e) {
            Thread.interrupted();
            LOGGER.info("Received signal to stop, event loop will halt");
        }
        // XStream's receiveLCRCallback() doesn't reliably propagate exceptions, so we do that ourselves here
        catch (Exception e) {
            LOGGER.error("Process record: {},{}",
                    Objects.toString(record.getYstreamLcrInterface(), "null"),
                    Objects.toString(record.getTableMetadata(), "null"));
            LOGGER.error("Process record error: ", e);
            errorHandler.setProducerThrowable(e);
        }
    }

    private void processDml(YStreamDataChangeRecord record, YstreamDml dml) throws InterruptedException {

        if (dml.hasChunkData()) {
            // If the row has chunk data, the RowLCR cannot be immediately dispatched.
            // The handler needs to cache the current row and wait for the chunks to be delivered before
            // the event can be safely dispatched. See processChunk below.
            currentRecord = record;
            YstreamColumns newValues = record.getYstreamDml().getNewValues();
            newValues.getColumns().forEach(ystreamColumn -> {
                if (ystreamColumn.isOutRow()) {
                    outRowSize++;
                }
            });
        } else {
            // Since the row has no chunk data, it can be dispatched immediately.
            dispatchDataChangeEvent(record, null);
        }
    }

    private void dispatchDataChangeEvent(YStreamDataChangeRecord record, Map<String, Object> chunkValues) throws InterruptedException {
        LOGGER.debug("Processing DML event {}", record.getYstreamDml());
        boolean truncateTable = record.isTruncateTable();
        TableId tableId = getTableId(record);

        Table table = schema.tableFor(tableId);
        if (table == null) {
            table = getTable(tableId);
            if (table == null) return;
        }

        try {
            // Xstream does not provide any before state for LOB columns and so this map will be
            // populated here by column name with the OracleValueConverters.UNAVAILABLE_VALUE.
            Map<String, Object> oldChunkValues = new HashMap<>(0);

            if (chunkValues == null) {
                // Happens when dispatching an LCR without any chunk data.
                chunkValues = new HashMap<>(0);
            }

            // LCR events may arrive both with and without chunk data.
            //
            // For example a DELETE by a primary key on a table with LOB columns will not supply any
            // LOB chunk data. In other scenarios such as an UPDATE where a LOB column is modified,
            // the updated LOB value will be provided but the prior value will not be.
            //
            // So in either case, the values need to be serialized here such that any LOB column that
            // is not explicitly provided in the map is initialized with the unavailable value
            // marker object so its transformed correctly by the value converters.

            for (Column column : schema.getLobColumnsForTable(table.id())) {
                // again Xstream doesn't supply before state for LOB values; explicitly use unavailable value
                oldChunkValues.put(column.name(), YashanDBValueConverters.UNAVAILABLE_VALUE);
                if (!chunkValues.containsKey(column.name())) {
                    // Column not supplied, initialize with unavailable value marker
                    LOGGER.trace("\tColumn '{}' not supplied, initialized with unavailable value", column.name());
                    chunkValues.put(column.name(), YashanDBValueConverters.UNAVAILABLE_VALUE);
                }
            }
            Table tableFor;
            Object[] newValues;
            Object[] oldValues;
            try {
                tableFor = schema.tableFor(tableId);
                newValues = truncateTable ? null : YStreamChangeRecordEmitter.getColumnValues(tableFor, record.getYstreamDml().getNewValues(), chunkValues);
                oldValues = truncateTable ? null : YStreamChangeRecordEmitter.getColumnValues(tableFor, record.getYstreamDml().getOldValues(), oldChunkValues);
            } catch (IllegalStateException e) {
                LOGGER.warn("Try to read table metadata again to resolve the error when obtaining field values.");
                printDiffMetadata(record, tableId);
                schema.getTables().removeTable(tableId);
                tableFor = getTable(tableId);
                newValues = YStreamChangeRecordEmitter.getColumnValues(tableFor, record.getYstreamDml().getNewValues(), chunkValues);
                oldValues = YStreamChangeRecordEmitter.getColumnValues(tableFor, record.getYstreamDml().getOldValues(), oldChunkValues);
            }
            if (!truncateTable) {
                //获取被修改的字段名称
                Set<String> columnNamesPresentInAfter = record.getYstreamDml().getNewValues().getColumns().stream()
                        .filter(ystreamColumn -> !ystreamColumn.getColumn().isDeleted())
                        .map(ystreamColumn -> ystreamColumn.getColumn().getColumnName())
                        .collect(Collectors.toSet());
                YStreamChangeRecordEmitter.calculateColumnValues(oldValues, newValues, columnNamesPresentInAfter, tableFor);
                //YStreamChangeRecordEmitter.calculateColumnValues(oldValues, newValues);
            }
            dispatcher.dispatchDataChangeEvent(
                    partition,
                    tableId,
                    new YStreamChangeRecordEmitter(
                            connectorConfig,
                            partition,
                            offsetContext,
                            record,
                            tableFor,
                            schema,
                            clock, newValues, oldValues));
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            printDiffMetadata(record, tableId);
            throw e;
        }
    }

    private void printDiffMetadata(YStreamDataChangeRecord record, TableId tableId) {
        String columnsInSchema = schema.tableFor(tableId).columns().stream().map(Column::name).collect(Collectors.joining(","));
        String columnsOnTable = record.getYstreamDml().getNewValues().getColumns().stream()
                .map(YstreamColumn::getColumn)
                .map(com.sics.ystream.metadata.Column::getColumnName)
                .collect(Collectors.joining(","));
        LOGGER.error("Dispatch data change event error,record schema: {}," +
                "table: {}," +
                "columns in the schema: {}," +
                "columns on the table: {}", tableId.schema(), tableId.table(), columnsInSchema, columnsOnTable);
    }

    public Table getTable(TableId tableId) throws InterruptedException {
        if (!connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
            LOGGER.trace("Table {} is new but excluded, schema change skipped.", tableId);
            return null;
        }

        LOGGER.warn("Obtaining schema for table {}, which should be already loaded, this may signal potential bug in fetching table schemas.", tableId);
        final String tableDdl;
        tableDdl = getTableMetadataDdl(tableId);

        LOGGER.info("Table {} will be captured.", tableId);
        dispatcher.dispatchSchemaChangeEvent(
                partition,
                offsetContext,
                tableId,
                new YashanDBSchemaChangeEventEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        tableId,
                        tableId.catalog(),
                        tableId.schema(),
                        tableDdl,
                        schema,
                        Instant.now(),
                        streamingMetrics,
                        null));

        return schema.tableFor(tableId);
    }

    private void dispatchSchemaChangeEvent(YstreamDdl ddl) throws InterruptedException {
        LOGGER.debug("Processing DDL event {}", ddl.getDdlText());

        TableId tableId = getTableId(ddl);

        dispatcher.dispatchSchemaChangeEvent(
                partition,
                offsetContext,
                tableId,
                new YashanDBSchemaChangeEventEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        tableId,
                        "",
                        ddl.getTableId().getSchema(),
                        ddl.getDdlText(),
                        schema,
                        ddl.getCurrentScn().getTimestamp().toInstant(),
                        streamingMetrics,
                        () -> processTruncateEvent(ddl)));
    }

    private void processTruncateEvent(YstreamDdl ddl) {
        LOGGER.debug("Handling truncate event");
        YStreamDataChangeRecord streamDmlRecord = new YStreamDataChangeRecord(new YStreamTruncate(ddl.getSize(),
                ddl.getPosition(),
                ddl.getSessionId(),
                ddl.getTableId(),
                ddl.getOldTableId(),
                ddl.isRecover(),
                ddl.getTransactionId(),
                ddl.getDdlType(),
                ddl.getObjectType(),
                ddl.getSsn(),
                ddl.getCurrentScn(),
                ddl.getDdlText(),
                ddl.getYstreamMetadata()));
        try {
            dispatchDataChangeEvent(streamDmlRecord, null);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted", e);
        }

    }

    private TableId getTableId(YStreamDataChangeRecord dmlRecord) {
        return dmlRecord.isTruncateTable() ? new TableId("", dmlRecord.getyStreamTruncate().getTableId().getSchema(), dmlRecord.getyStreamTruncate().getTableId().getTable())
                : new TableId("", dmlRecord.getYstreamDml().getTableId().getSchema(), dmlRecord.getYstreamDml().getTableId().getTable());
    }

    private TableId getTableId(YstreamDdl ddl) {
        return new TableId("", ddl.getTableId().getSchema(), ddl.getTableId().getTable());
    }

    private String getTableMetadataDdl(TableId tableId) {
        LOGGER.info("Getting database metadata for table '{}'", tableId);
        // A separate connection must be used for this out-of-bands query while processing the Xstream callback.
        // This should have negligible overhead as this should happen rarely.
        try (YashanDBConnection connection = new YashanDBConnection(connectorConfig.getJdbcConfig())) {
            connection.setAutoCommit(false);
            String tableMetadataDdl = connection.getTableMetadataDdl(tableId);
            LOGGER.debug("Obtain table {}.{} ddl: {}", tableId.schema(), tableId.table(), tableMetadataDdl);
            return tableMetadataDdl;
        } catch (SQLException e) {
            throw new DebeziumException("Failed to get table DDL metadata for: " + tableId, e);
        }
    }

    private void setWatermark() {
        if (eventSource.getYstreamClientBoot() == null) {
            return;
        }
        final YStreamStreamingChangeEventSource.PositionAndScn message = eventSource.receivePublishedPosition();
        if (message == null) {
            return;
        }
        LOGGER.debug("Recording offsets to YashanDB");
        if (message.position != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Recording position {}", message.position);
            }
            eventSource.getYstreamClientBoot().setAppliedPosition(
                    message.position.getRawPosition());
        } else if (message.scn != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Recording position with SCN {}", message.scn);
            }
        } else {
            LOGGER.warn("Nothing in offsets could be recorded to YashanDB");
            return;
        }
        LOGGER.trace("Offsets recorded to YashanDB");
    }

    public void processChunk(YstreamChunk chunk) {
        columnChunks.computeIfAbsent(chunk.getColumn().getColumnName(), v -> new ChunkColumnValues()).add(chunk);
        if (chunk.isEnd()) {
            outRowSize--;
            if (outRowSize == 0) {
                resolveAndDispatchCurrentChunkedRow();
            }
        }
    }

    private void resolveAndDispatchCurrentChunkedRow() {
        try {
            // Map of resolved chunk values
            Map<String, Object> resolvedChunkValues = new HashMap<>();

            // All chunks have been dispatched to the event handler, combine the chunks now.
            for (Map.Entry<String, ChunkColumnValues> entry : columnChunks.entrySet()) {
                final String columnName = entry.getKey();
                final ChunkColumnValues chunkValues = entry.getValue();

                if (chunkValues.isEmpty()) {
                    LOGGER.trace("Column '{}' has no chunk values.", columnName);
                    continue;
                }

                final int type = chunkValues.getChunkType();
                switch (type) {
                    case YasTypes.CLOB:
                    case YasTypes.NCLOB:
                        resolvedChunkValues.put(columnName, chunkValues.getStringValue(currentRecord.getTableMetadata()));
                        break;

                    case YasTypes.SQLXML:
                        resolvedChunkValues.put(columnName, chunkValues.getXmlValue());
                        break;

                    case YasTypes.RAW:
                    case YasTypes.BLOB:
                        resolvedChunkValues.put(columnName, chunkValues.getByteArray());
                        break;

                    default:
                        LOGGER.trace("Received an unsupported chunk type '{}' for column '{}', ignored.", type, columnName);
                        break;
                }
            }

            columnChunks.clear();
            dispatchDataChangeEvent(new YStreamDataChangeRecord((YstreamDml) currentRecord.getYstreamLcrInterface(), currentRecord.getTableMetadata()),
                    resolvedChunkValues);
        } catch (InterruptedException e) {
            Thread.interrupted();
            LOGGER.info("Received signal to stop, event loop will halt");
        } catch (SQLException e) {
            throw new DebeziumException("Failed to process chunk data", e);
        }
    }
}