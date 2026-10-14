/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.connector.yashandb.ystream.TruncateReceiver;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParserListener;
import io.debezium.relational.ddl.DdlParserListener.TableAlteredEvent;
import io.debezium.relational.ddl.DdlParserListener.TableCreatedEvent;
import io.debezium.relational.ddl.DdlParserListener.TableDroppedEvent;
import io.debezium.relational.ddl.DdlParserListener.TableEvent;
import io.debezium.relational.ddl.DdlParserListener.TableTruncatedEvent;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;

/**
 * {@link SchemaChangeEventEmitter} implementation based on YashanDB.
 */
public class YashanDbSchemaChangeEventEmitter implements SchemaChangeEventEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDbSchemaChangeEventEmitter.class);

    private final YashanDbPartition partition;
    private final YashanDbOffsetContext offsetContext;
    private final TableId tableId;
    private final YashanDbDatabaseSchema schema;
    private final Instant changeTime;
    private final String sourceDatabaseName;
    private final String objectOwner;
    private final String ddlText;
    private final TableFilter filters;
    private final YashanDbStreamingChangeEventSourceMetrics streamingMetrics;
    private final TruncateReceiver truncateReceiver;
    private final YashanDbConnectorConfig connectorConfig;

    /**
     * Creates a new schema change event emitter.
     *
     * @param connectorConfig the connector configuration
     * @param partition the partition
     * @param offsetContext the offset context
     * @param tableId the table identifier
     * @param sourceDatabaseName the source database name
     * @param objectOwner the object owner
     * @param ddlText the DDL text to emit
     * @param schema the database schema
     * @param changeTime the change timestamp
     * @param streamingMetrics the streaming metrics
     * @param truncateReceiver the truncate event receiver
     */
    public YashanDbSchemaChangeEventEmitter(YashanDbConnectorConfig connectorConfig, YashanDbPartition partition,
                                            YashanDbOffsetContext offsetContext, TableId tableId, String sourceDatabaseName,
                                            String objectOwner, String ddlText, YashanDbDatabaseSchema schema,
                                            Instant changeTime, YashanDbStreamingChangeEventSourceMetrics streamingMetrics,
                                            TruncateReceiver truncateReceiver) {
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.tableId = tableId;
        this.sourceDatabaseName = sourceDatabaseName;
        this.objectOwner = objectOwner;
        this.ddlText = ddlText;
        this.schema = schema;
        this.changeTime = changeTime;
        this.streamingMetrics = streamingMetrics;
        this.filters = connectorConfig.getTableFilters().dataCollectionFilter();
        this.truncateReceiver = truncateReceiver;
        this.connectorConfig = connectorConfig;
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {
        // The parser removes dropped tables before it returns their events.
        final Map<TableId, Table> tablesBefore = new HashMap<>();
        schema.getTables().tableIds().forEach(id -> tablesBefore.put(id, schema.getTables().forTable(id)));

        final AntlrDdlParser<?, ?> parser = schema.getDdlParser();
        final DdlChanges ddlChanges;
        try {
            parser.getAndResetDdlChanges();
            parser.setCurrentDatabase(sourceDatabaseName);
            parser.setCurrentSchema(objectOwner);
            ddlChanges = parser.parse(ddlText, schema.getTables());
        }
        catch (ParsingException | MultipleParsingExceptions e) {
            if (schema.skipUnparseableDdlStatements()) {
                LOGGER.warn("Ignoring unparsable DDL statement '{}'", ddlText, e);
                streamingMetrics.incrementWarningCount();
                streamingMetrics.incrementUnparsableDdlCount();
                if (connectorConfig.getDdlParseFailRetryReadTable()) {
                    LOGGER.warn("DDL parsing failed. Next time when performing DML synchronization, the full table structure will be read and parsed.");
                    schema.getTables().removeTable(tableId);
                }
                return;
            }
            else {
                throw e;
            }
        }

        if (!ddlChanges.isEmpty()) {
            List<SchemaChangeEvent> changeEvents = new ArrayList<>();
            ddlChanges.getEventsByDatabase((String dbName, List<DdlParserListener.Event> events) -> {
                events.forEach(event -> {
                    if (!(event instanceof TableEvent)) {
                        return;
                    }
                    TableId eventTableId = ((TableEvent) event).tableId();
                    if (schema.storeOnlyCapturedTables() && !filters.isIncluded(eventTableId)
                            && !(event instanceof TableAlteredEvent
                                    && ((TableAlteredEvent) event).previousTableId() != null
                                    && filters.isIncluded(((TableAlteredEvent) event).previousTableId()))) {
                        return;
                    }
                    switch (event.type()) {
                        case CREATE_TABLE:
                            changeEvents.add(createTableEvent(partition, (TableCreatedEvent) event));
                            break;
                        case ALTER_TABLE:
                            changeEvents.add(alterTableEvent(partition, (TableAlteredEvent) event));
                            break;
                        case DROP_TABLE:
                            Table tableBefore = tablesBefore.get(eventTableId);
                            if (tableBefore != null) {
                                changeEvents.add(dropTableEvent(partition, tableBefore, (TableDroppedEvent) event));
                            }
                            break;
                        case TRUNCATE_TABLE:
                            changeEvents.add(truncateTableEvent(partition, (TableTruncatedEvent) event));
                            break;
                        default:
                            LOGGER.info("Skipped DDL event type {}: {}", event.type(), ddlText);
                            break;
                    }
                });
            });

            for (SchemaChangeEvent event : changeEvents) {
                if (!schema.skipSchemaChangeEvent(event)) {
                    if (SchemaChangeEvent.SchemaChangeEventType.TRUNCATE == event.getType()) {
                        truncateReceiver.processTruncateEvent();
                    }
                    else {
                        receiver.schemaChangeEvent(event);
                    }

                }
            }
        }
    }

    private SchemaChangeEvent createTableEvent(YashanDbPartition partition, TableCreatedEvent event) {
        TableId eventTableId = event.tableId();
        offsetContext.tableEvent(tableId, changeTime);
        return SchemaChangeEvent.ofCreate(
                partition,
                offsetContext,
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                schema.getTables().forTable(eventTableId),
                false);
    }

    private SchemaChangeEvent alterTableEvent(YashanDbPartition partition, TableAlteredEvent event) {
        final TableId previousTableId = event.previousTableId();
        final Set<TableId> tableIds = new LinkedHashSet<>();
        tableIds.add(event.tableId());
        if (previousTableId != null) {
            tableIds.add(previousTableId);
        }

        offsetContext.tableEvent(tableIds, changeTime);
        if (previousTableId == null) {
            return SchemaChangeEvent.ofAlter(
                    partition,
                    offsetContext,
                    tableId.catalog(),
                    tableId.schema(),
                    event.statement(),
                    schema.tableFor(event.tableId()));
        }
        else {
            return SchemaChangeEvent.ofRename(
                    partition,
                    offsetContext,
                    tableId.catalog(),
                    tableId.schema(),
                    event.statement(),
                    schema.getTables().forTable(event.tableId()),
                    previousTableId);
        }
    }

    private SchemaChangeEvent dropTableEvent(YashanDbPartition partition, Table tableSchemaBeforeDrop, TableDroppedEvent event) {
        offsetContext.tableEvent(tableId, changeTime);
        return SchemaChangeEvent.ofDrop(
                partition,
                offsetContext,
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                tableSchemaBeforeDrop);
    }

    private SchemaChangeEvent truncateTableEvent(YashanDbPartition partition, TableTruncatedEvent event) {
        TableId eventTableId = event.tableId();
        offsetContext.tableEvent(tableId, changeTime);
        return SchemaChangeEvent.ofTruncate(
                partition,
                offsetContext,
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                schema.tableFor(eventTableId));
    }

}
