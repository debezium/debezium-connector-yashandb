/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import io.debezium.connector.yashandb.antlr.YashanDBDdlParser;
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
import io.debezium.relational.ddl.DdlParserListener.TableTruncatedEvent;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link SchemaChangeEventEmitter} implementation based on YashanDB.
 *
 * TODO: delete this?
 */
public class YashanDBSchemaChangeEventEmitter implements SchemaChangeEventEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDBSchemaChangeEventEmitter.class);

    private final YashanDBPartition partition;
    private final YashanDBOffsetContext offsetContext;
    private final TableId tableId;
    private final YashanDBDatabaseSchema schema;
    private final Instant changeTime;
    private final String sourceDatabaseName;
    private final String objectOwner;
    private final String ddlText;
    private final TableFilter filters;
    private final YashanDBStreamingChangeEventSourceMetrics streamingMetrics;
    private final TruncateReceiver truncateReceiver;
    private final YashanDBConnectorConfig connectorConfig;

    public YashanDBSchemaChangeEventEmitter(YashanDBConnectorConfig connectorConfig, YashanDBPartition partition,
                                            YashanDBOffsetContext offsetContext, TableId tableId, String sourceDatabaseName,
                                            String objectOwner, String ddlText, YashanDBDatabaseSchema schema,
                                            Instant changeTime, YashanDBStreamingChangeEventSourceMetrics streamingMetrics,
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
        // Cache a copy of the table's schema prior to parsing the DDL.
        // This is needed in the event that the parsed DDL is a drop table
        // todo: verify whether this is actually necessary in the emitted SchemaChangeEvent
        final Table tableBefore = schema.tableFor(tableId);

        final YashanDBDdlParser parser = schema.getDdlParser();
        final DdlChanges ddlChanges = parser.getDdlChanges();
        try {
            ddlChanges.reset();
            parser.setCurrentDatabase(sourceDatabaseName);
            parser.setCurrentSchema(objectOwner);
            parser.parse(ddlText, schema.getTables());
        } catch (ParsingException | MultipleParsingExceptions e) {
            if (schema.skipUnparseableDdlStatements()) {
                LOGGER.warn("Ignoring unparsable DDL statement '{}'", ddlText, e);
                streamingMetrics.incrementWarningCount();
                streamingMetrics.incrementUnparsableDdlCount();
                if (connectorConfig.getDdlParseFailRetryReadTable()) {
                    if (tableId != null) {
                        LOGGER.warn("DDL parsing failed. Next time when performing DML synchronization, the full table structure will be read and parsed.");
                        schema.getTables().removeTable(tableId);
                    }
                }
            } else {
                throw e;
            }
        }

        if (!ddlChanges.isEmpty() && (filters.isIncluded(tableId) || !schema.storeOnlyCapturedTables())) {
            List<SchemaChangeEvent> changeEvents = new ArrayList<>();
            ddlChanges.getEventsByDatabase((String dbName, List<DdlParserListener.Event> events) -> {
                events.forEach(event -> {
                    switch (event.type()) {
                        case CREATE_TABLE:
                            changeEvents.add(createTableEvent(partition, (TableCreatedEvent) event));
                            break;
                        case ALTER_TABLE:
                            changeEvents.add(alterTableEvent(partition, (TableAlteredEvent) event));
                            break;
                        case DROP_TABLE:
                            changeEvents.add(dropTableEvent(partition, tableBefore, (TableDroppedEvent) event));
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
                    } else {
                        receiver.schemaChangeEvent(event);
                    }

                }
            }
        }
    }

    private SchemaChangeEvent createTableEvent(YashanDBPartition partition, TableCreatedEvent event) {
        offsetContext.tableEvent(tableId, changeTime);
        return SchemaChangeEvent.ofCreate(
                partition,
                offsetContext,
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                schema.tableFor(event.tableId()),
                false);
    }

    private SchemaChangeEvent alterTableEvent(YashanDBPartition partition, TableAlteredEvent event) {
        final Set<TableId> tableIds = new LinkedHashSet<>();
        tableIds.add(tableId);
        tableIds.add(event.tableId());

        offsetContext.tableEvent(tableIds, changeTime);
        if (tableId == null) {
            return SchemaChangeEvent.ofAlter(
                    partition,
                    offsetContext,
                    tableId.catalog(),
                    tableId.schema(),
                    event.statement(),
                    schema.tableFor(event.tableId()));
        } else {
            return SchemaChangeEvent.ofRename(
                    partition,
                    offsetContext,
                    tableId.catalog(),
                    tableId.schema(),
                    event.statement(),
                    schema.tableFor(event.tableId()),
                    tableId);
        }
    }

    private SchemaChangeEvent dropTableEvent(YashanDBPartition partition, Table tableSchemaBeforeDrop, TableDroppedEvent event) {
        offsetContext.tableEvent(tableId, changeTime);
        return SchemaChangeEvent.ofDrop(
                partition,
                offsetContext,
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                tableSchemaBeforeDrop);
    }

    private SchemaChangeEvent truncateTableEvent(YashanDBPartition partition, TableTruncatedEvent event) {
        offsetContext.tableEvent(tableId, changeTime);
        return SchemaChangeEvent.ofTruncate(
                partition,
                offsetContext,
                tableId.catalog(),
                tableId.schema(),
                event.statement(),
                schema.tableFor(event.tableId()));
    }

}
