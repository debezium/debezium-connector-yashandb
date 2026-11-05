/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.yashandb.antlr.YashanDbDdlParser;
import io.debezium.connector.yashandb.antlr.YashanDbMySqlDdlParser;
import io.debezium.connector.yashandb.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter.Receiver;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.TableId;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.SchemaChangeEvent;

class YashanDbSchemaChangeEventEmitterTest {

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldEmitMySqlChangesUsingParsedTableIds() throws InterruptedException {
        YashanDbConnectorConfig config = spy(new YashanDbConnectorConfig(TestHelper.defaultConfig()
                .with("ystream.additional.properties.mysql.mode", "true").build()));
        SchemaHistory history = mock(SchemaHistory.class);
        YashanDbDatabaseSchema schema = createSchema(config, history);
        assertThat(schema.getDdlParser()).isInstanceOf(YashanDbMySqlDdlParser.class);
        YashanDbOffsetContext offset = offset();
        YashanDbPartition partition = new YashanDbPartition("server", "shop");
        List<SchemaChangeEvent> events = new ArrayList<>();
        Receiver receiver = event -> {
            schema.applySchemaChange(event);
            events.add(event);
        };
        TableId sourceId = new TableId(null, "shop", "orders");

        emit(config, partition, offset, sourceId, "CREATE TABLE orders (id INT PRIMARY KEY)", schema, receiver);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).getTables()).singleElement().satisfies(table -> assertThat(table.id()).isEqualTo(sourceId));
        assertThat(schema.schemaFor(sourceId)).isNotNull();

        emit(config, partition, offset, sourceId, "ALTER TABLE orders ADD COLUMN note VARCHAR(20)", schema, receiver);
        assertThat(events).hasSize(2);
        assertThat(events.get(1).getType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.ALTER);
        assertThat(events.get(1).getTableChanges()).singleElement()
                .satisfies(change -> assertThat(change.getPreviousId()).isNull());
        assertThat(events.get(1).getTables()).singleElement().satisfies(table -> assertThat(table.columnWithName("note")).isNotNull());
        assertThat(schema.schemaFor(sourceId).valueSchema().field("note")).isNotNull();

        emit(config, partition, offset, sourceId, "ALTER TABLE orders RENAME TO purchases", schema, receiver);
        TableId renamedId = new TableId(null, "shop", "purchases");
        assertThat(events).hasSize(3);
        assertThat(events.get(2).getTableChanges()).singleElement().satisfies(change -> {
            assertThat(change.getId()).isEqualTo(renamedId);
            assertThat(change.getPreviousId()).isEqualTo(sourceId);
        });
        assertThat(schema.schemaFor(sourceId)).isNull();
        assertThat(schema.schemaFor(renamedId)).isNotNull();

        emit(config, partition, offset, sourceId, "DROP TABLE purchases", schema, receiver);
        assertThat(events).hasSize(4);
        assertThat(events.get(3).getType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.DROP);
        assertThat(events.get(3).getTableChanges()).singleElement().satisfies(change -> assertThat(change.getId()).isEqualTo(renamedId));
        assertThat(schema.schemaFor(renamedId)).isNull();
        assertThat(schema.getTables().forTable(renamedId)).isNull();
        verify(history, times(4)).record(any(), any(), isNull(), eq("shop"), any(), any(TableChanges.class), any());
        schema.close();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldUseEmitterTableIdForTruncateEventOffset() throws InterruptedException {
        YashanDbConnectorConfig config = spy(new YashanDbConnectorConfig(TestHelper.defaultConfig()
                .with("ystream.additional.properties.mysql.mode", "true").build()));
        try (YashanDbDatabaseSchema schema = createSchema(config, mock(SchemaHistory.class))) {
            YashanDbPartition partition = new YashanDbPartition("server", "shop");
            TableId tableId = new TableId(null, "shop", "orders");
            emit(config, partition, offset(), tableId, "CREATE TABLE orders (id INT PRIMARY KEY)", schema,
                    schema::applySchemaChange);

            YashanDbOffsetContext offset = offset();
            AtomicBoolean truncateReceived = new AtomicBoolean();
            YashanDbSchemaChangeEventEmitter emitter = new YashanDbSchemaChangeEventEmitter(
                    config,
                    partition,
                    offset,
                    tableId,
                    "",
                    tableId.schema(),
                    "TRUNCATE TABLE orders",
                    schema,
                    Instant.now(),
                    mock(YashanDbStreamingChangeEventSourceMetrics.class),
                    () -> truncateReceived.set(true));

            emitter.emitSchemaChangeEvent(event -> {
                throw new AssertionError("Truncate event should be delivered to the truncate receiver");
            });

            assertThat(truncateReceived).isTrue();
            verify(offset).tableEvent(eq(tableId), any(Instant.class));
        }
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldRecordRenameOutOfCapturedTables() throws InterruptedException {
        YashanDbConnectorConfig config = spy(new YashanDbConnectorConfig(TestHelper.defaultConfig()
                .with("ystream.additional.properties.mysql.mode", "true")
                .with("table.include.list", "shop.orders")
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true).build()));
        SchemaHistory history = mock(SchemaHistory.class);
        try (YashanDbDatabaseSchema schema = createSchema(config, history)) {
            YashanDbPartition partition = new YashanDbPartition("server", "shop");
            TableId id = new TableId(null, "shop", "orders");
            emit(config, partition, offset(), id, "CREATE TABLE orders (id INT PRIMARY KEY)", schema, schema::applySchemaChange);
            emit(config, partition, offset(), id, "RENAME TABLE orders TO archived", schema, schema::applySchemaChange);

            assertThat(schema.schemaFor(id)).isNull();
            assertThat(schema.getTables().forTable(id)).isNull();
            ArgumentCaptor<TableChanges> changes = ArgumentCaptor.forClass(TableChanges.class);
            verify(history, times(2)).record(any(), any(), isNull(), eq("shop"), any(), changes.capture(), any());
            assertThat(changes.getValue()).singleElement().satisfies(change -> {
                assertThat(change.getPreviousId()).isEqualTo(id);
                assertThat(change.getId()).isEqualTo(new TableId(null, "shop", "archived"));
            });
        }
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldRetainUncapturedTableDefinitionsInHistoryAndDropMultipleTables() throws InterruptedException {
        YashanDbConnectorConfig config = spy(new YashanDbConnectorConfig(TestHelper.defaultConfig()
                .with("ystream.additional.properties.mysql.mode", "true")
                .with("table.include.list", "shop.orders")
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, false).build()));
        SchemaHistory history = mock(SchemaHistory.class);
        try (YashanDbDatabaseSchema schema = createSchema(config, history)) {
            YashanDbPartition partition = new YashanDbPartition("server", "shop");
            TableId id = new TableId(null, "shop", "orders");
            List<SchemaChangeEvent> events = new ArrayList<>();
            Receiver receiver = event -> {
                schema.applySchemaChange(event);
                events.add(event);
            };
            emit(config, partition, offset(), id, "CREATE TABLE orders (id INT)", schema, receiver);
            emit(config, partition, offset(), id, "CREATE TABLE excluded (id INT)", schema, receiver);
            assertThat(events.get(1).getTableChanges()).hasSize(1);
            assertThat(schema.tableFor(new TableId(null, "shop", "excluded"))).isNull();

            emit(config, partition, offset(), id, "DROP TABLE orders, excluded", schema, receiver);
            assertThat(events).hasSize(4);
            assertThat(events.subList(2, 4)).allSatisfy(event -> {
                assertThat(event.getType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.DROP);
                assertThat(event.getTableChanges()).hasSize(1);
            });
            assertThat(schema.getTables().tableIds()).isEmpty();
            verify(history, times(4)).record(any(), any(), isNull(), eq("shop"), any(), any(TableChanges.class), any());
        }
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldSelectNativeParserOutsideMySqlMode() {
        YashanDbConnectorConfig config = spy(new YashanDbConnectorConfig(TestHelper.defaultConfig().build()));
        try (YashanDbDatabaseSchema schema = createSchema(config, mock(SchemaHistory.class))) {
            assertThat(schema.getDdlParser()).isInstanceOf(YashanDbDdlParser.class);
        }
    }

    private YashanDbDatabaseSchema createSchema(YashanDbConnectorConfig config, SchemaHistory history) {
        doReturn(history).when(config).getSchemaHistory();
        return new YashanDbDatabaseSchema(config, YashanDbConnectorTask.createValueConverters(config, null),
                DefaultValueConverter.passthrough(), config.schemaNameAdjuster(),
                config.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY),
                StreamingAdapter.TableNameCaseSensitivity.SENSITIVE, new CustomConverterRegistry(List.of()),
                new YashanDbTaskContext(Configuration.create().build(), config));
    }

    private YashanDbOffsetContext offset() {
        YashanDbOffsetContext offset = mock(YashanDbOffsetContext.class);
        doReturn(Map.of("scn", "1")).when(offset).getOffset();
        when(offset.getSourceInfo()).thenReturn(new Struct(SchemaBuilder.struct().name("source").build()));
        return offset;
    }

    private void emit(YashanDbConnectorConfig config, YashanDbPartition partition, YashanDbOffsetContext offset,
                      TableId sourceId, String ddl, YashanDbDatabaseSchema schema, Receiver receiver)
            throws InterruptedException {
        new YashanDbSchemaChangeEventEmitter(config, partition, offset, sourceId, "", sourceId.schema(), ddl,
                schema, Instant.now(), mock(YashanDbStreamingChangeEventSourceMetrics.class), () -> {
                }).emitSchemaChangeEvent(receiver);
    }
}
