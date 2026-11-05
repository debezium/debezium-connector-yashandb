/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.sics.ystream.result.LcrType;
import com.sics.ystream.result.SystemChangeNumber;
import com.sics.ystream.result.YstreamDdl;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.yashandb.StreamingAdapter;
import io.debezium.connector.yashandb.YashanDbConnectorConfig;
import io.debezium.connector.yashandb.YashanDbDatabaseSchema;
import io.debezium.connector.yashandb.YashanDbMySqlValueConverters;
import io.debezium.connector.yashandb.YashanDbOffsetContext;
import io.debezium.connector.yashandb.YashanDbPartition;
import io.debezium.connector.yashandb.YashanDbStreamingChangeEventSourceMetrics;
import io.debezium.connector.yashandb.YashanDbTaskContext;
import io.debezium.connector.yashandb.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.TableId;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.Clock;

class YStreamMySqlDdlTest {

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldDispatchRenameWhenLcrContainsExcludedNewName() {
        YashanDbConnectorConfig config = config();
        SchemaHistory history = mock(SchemaHistory.class);
        try (YashanDbDatabaseSchema schema = schema(config, history)) {
            ErrorHandler errorHandler = mock(ErrorHandler.class);
            YStreamEventHandler handler = handler(config, schema, errorHandler);
            TableId oldId = new TableId(null, "shop", "orders");
            handler.processRecord(record("orders", "CREATE TABLE orders (id INT PRIMARY KEY)"));
            assertThat(schema.schemaFor(oldId)).isNotNull();

            handler.processRecord(record("archived", "RENAME TABLE orders TO archived"));

            assertThat(schema.schemaFor(oldId)).isNull();
            assertThat(schema.getTables().forTable(oldId)).isNull();
            ArgumentCaptor<TableChanges> changes = ArgumentCaptor.forClass(TableChanges.class);
            verify(history, times(2)).record(any(), any(), eq(""), eq("shop"), any(), changes.capture(), any());
            assertThat(changes.getValue()).singleElement().satisfies(change -> {
                assertThat(change.getPreviousId()).isEqualTo(oldId);
                assertThat(change.getId()).isEqualTo(new TableId(null, "shop", "archived"));
            });
            verifyNoInteractions(errorHandler);
        }
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldDispatchMultiTableDropWhenLcrTableIsExcluded() {
        YashanDbConnectorConfig config = config();
        SchemaHistory history = mock(SchemaHistory.class);
        try (YashanDbDatabaseSchema schema = schema(config, history)) {
            ErrorHandler errorHandler = mock(ErrorHandler.class);
            YStreamEventHandler handler = handler(config, schema, errorHandler);
            TableId id = new TableId(null, "shop", "orders");
            handler.processRecord(record("orders", "CREATE TABLE orders (id INT PRIMARY KEY)"));
            assertThat(schema.schemaFor(id)).isNotNull();

            handler.processRecord(record("excluded", "DROP TABLE IF EXISTS excluded, orders"));

            assertThat(schema.schemaFor(id)).isNull();
            assertThat(schema.getTables().forTable(id)).isNull();
            ArgumentCaptor<TableChanges> changes = ArgumentCaptor.forClass(TableChanges.class);
            verify(history, times(2)).record(any(), any(), eq(""), eq("shop"), any(), changes.capture(), any());
            assertThat(changes.getValue()).singleElement().satisfies(change -> {
                assertThat(change.getType()).isEqualTo(TableChanges.TableChangeType.DROP);
                assertThat(change.getId()).isEqualTo(id);
            });
            verifyNoInteractions(errorHandler);
        }
    }

    private YashanDbConnectorConfig config() {
        return spy(new YashanDbConnectorConfig(TestHelper.defaultConfig()
                .with("ystream.additional.properties.mysql.mode", "true")
                .with("table.include.list", "shop.orders")
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true).build()));
    }

    private YashanDbDatabaseSchema schema(YashanDbConnectorConfig config, SchemaHistory history) {
        doReturn(history).when(config).getSchemaHistory();
        return new YashanDbDatabaseSchema(config, new YashanDbMySqlValueConverters(config, null),
                DefaultValueConverter.passthrough(), config.schemaNameAdjuster(),
                config.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY),
                StreamingAdapter.TableNameCaseSensitivity.SENSITIVE, new CustomConverterRegistry(List.of()),
                new YashanDbTaskContext(Configuration.create().build(), config));
    }

    private YStreamEventHandler handler(YashanDbConnectorConfig config, YashanDbDatabaseSchema schema, ErrorHandler errorHandler) {
        EventDispatcher<YashanDbPartition, TableId> dispatcher = new EventDispatcher<>(config,
                config.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY), schema, null,
                config.getTableFilters().dataCollectionFilter(), null, mock(EventMetadataProvider.class), config.schemaNameAdjuster(), null);
        YashanDbOffsetContext offset = mock(YashanDbOffsetContext.class);
        when(offset.getLcrPosition()).thenReturn(YStreamPosition.valueOf(1L).getLcrPosition());
        doReturn(Map.of("scn", "1")).when(offset).getOffset();
        when(offset.getSourceInfo()).thenReturn(new Struct(SchemaBuilder.struct().name("source").build()));
        return new YStreamEventHandler(config, errorHandler, dispatcher, Clock.system(), schema,
                new YashanDbPartition("server", "shop"), offset, mock(YStreamStreamingChangeEventSource.class),
                mock(YashanDbStreamingChangeEventSourceMetrics.class));
    }

    private YStreamRecord record(String table, String sql) {
        YstreamDdl ddl = mock(YstreamDdl.class);
        com.sics.ystream.metadata.TableId id = mock(com.sics.ystream.metadata.TableId.class);
        when(id.getSchema()).thenReturn("shop");
        when(id.getTable()).thenReturn(table);
        when(ddl.getTableId()).thenReturn(id);
        when(ddl.getPosition()).thenReturn(YStreamPosition.valueOf(2L).getLcrPosition());
        when(ddl.getLcrType()).thenReturn(LcrType.YSTREAM_DDL);
        when(ddl.getDdlText()).thenReturn(sql);
        SystemChangeNumber scn = mock(SystemChangeNumber.class);
        when(scn.getTimestamp()).thenReturn(Timestamp.from(Instant.now()));
        when(ddl.getCurrentScn()).thenReturn(scn);
        return new YStreamRecord(ddl, null);
    }
}
