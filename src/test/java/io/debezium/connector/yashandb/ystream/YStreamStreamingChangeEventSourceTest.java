/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.sics.ystream.YstreamClientBoot;
import com.sics.ystream.metadata.TableId;
import com.sics.ystream.result.LcrType;
import com.sics.ystream.result.LogPosition;
import com.sics.ystream.result.Position;
import com.sics.ystream.result.SystemChangeNumber;
import com.sics.ystream.result.YstreamMetadata;

import io.debezium.connector.yashandb.YashanDbConnection;
import io.debezium.connector.yashandb.YashanDbConnectorConfig;
import io.debezium.connector.yashandb.YashanDbDatabaseSchema;
import io.debezium.connector.yashandb.YashanDbOffsetContext;
import io.debezium.connector.yashandb.YashanDbPartition;
import io.debezium.connector.yashandb.YashanDbStreamingChangeEventSourceMetrics;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.util.Clock;

class YStreamStreamingChangeEventSourceTest {

    private YashanDbConnectorConfig connectorConfig;
    private YashanDbConnection jdbcConnection;
    private EventDispatcher<YashanDbPartition, ?> dispatcher;
    private ErrorHandler errorHandler;
    private YashanDbDatabaseSchema schema;
    private YashanDbStreamingChangeEventSourceMetrics streamingMetrics;
    private ChangeEventSourceContext context;
    private YashanDbOffsetContext offsetContext;
    private YstreamClientBoot<YStreamRecord> client;
    private YashanDbPartition partition;
    private Clock clock;

    @BeforeEach
    void setUp() {
        connectorConfig = mock(YashanDbConnectorConfig.class);
        jdbcConnection = mock(YashanDbConnection.class);
        dispatcher = mock(EventDispatcher.class);
        errorHandler = mock(ErrorHandler.class);
        schema = mock(YashanDbDatabaseSchema.class);
        streamingMetrics = mock(YashanDbStreamingChangeEventSourceMetrics.class);
        context = mock(ChangeEventSourceContext.class);
        offsetContext = mock(YashanDbOffsetContext.class);
        client = mock(YstreamClientBoot.class);
        partition = new YashanDbPartition("server", "database");

        AtomicLong time = new AtomicLong();
        clock = () -> time.addAndGet(1_000L);

        JdbcConfiguration jdbcConfiguration = mock(JdbcConfiguration.class);
        when(jdbcConnection.config()).thenReturn(jdbcConfiguration);
        when(jdbcConfiguration.getHostname()).thenReturn("localhost");
        when(jdbcConfiguration.getPort()).thenReturn(1688);
        when(jdbcConfiguration.getUser()).thenReturn("sys");
        when(jdbcConfiguration.getPassword()).thenReturn("password");

        when(connectorConfig.getYstreamServerName()).thenReturn("server");
        when(connectorConfig.getyStreamQueueSize()).thenReturn(16);
        when(connectorConfig.getyStreamPollTimeout()).thenReturn(1_000);
        when(connectorConfig.getyStreamClientResponseTimeout()).thenReturn(1_000);

        Position position = new Position(new SystemChangeNumber(1L), new LogPosition());
        when(offsetContext.getLcrPosition()).thenReturn(position);
        when(offsetContext.getRecoverPosition()).thenReturn(position);
    }

    @Test
    void shouldLimitCombinedOpenAndReadAttemptsToThirty() throws Exception {
        AtomicInteger openCalls = new AtomicInteger();
        doAnswer(invocation -> {
            if (openCalls.incrementAndGet() <= 29) {
                throw new RuntimeException("open failed");
            }
            return null;
        }).when(client).open(any());
        when(client.next()).thenThrow(new RuntimeException("read failed"));
        when(context.isRunning()).thenReturn(true);

        try (MockedStatic<YstreamClientBoot> factory = mockStatic(YstreamClientBoot.class)) {
            factory.when(() -> YstreamClientBoot.<YStreamRecord> getClient()).thenReturn(client);
            createEventSource().execute(context, partition, offsetContext);
        }

        verify(client, times(30)).open(any());
        verify(client, times(1)).next();
        verify(errorHandler).setProducerThrowable(any(RuntimeException.class));
    }

    @Test
    void shouldKeepRetryBudgetAfterReceivingRecord() throws Exception {
        AtomicInteger readCalls = new AtomicInteger();
        YstreamMetadata metadata = mock(YstreamMetadata.class);
        TableId tableId = mock(TableId.class);
        when(metadata.getLcrType()).thenReturn(LcrType.YSTREAM_METADATA);
        when(metadata.getTableId()).thenReturn(tableId);
        when(tableId.getSchema()).thenReturn("schema");
        when(tableId.getTable()).thenReturn("table");
        YStreamRecord record = new YStreamRecord(metadata, null);
        when(client.next()).thenAnswer(invocation -> {
            int readCall = readCalls.incrementAndGet();
            if (readCall == 30) {
                return record;
            }
            throw new RuntimeException("read failed");
        });
        when(context.isRunning()).thenReturn(true);

        try (MockedStatic<YstreamClientBoot> factory = mockStatic(YstreamClientBoot.class)) {
            factory.when(() -> YstreamClientBoot.<YStreamRecord> getClient()).thenReturn(client);
            createEventSource().execute(context, partition, offsetContext);
        }

        verify(client, times(31)).next();
        verify(errorHandler).setProducerThrowable(any(RuntimeException.class));
    }

    @Test
    void shouldPropagateInterruptedReadWithoutRetrying() throws Exception {
        when(client.next()).thenThrow(new InterruptedException("stop"));
        when(context.isRunning()).thenReturn(true);

        try (MockedStatic<YstreamClientBoot> factory = mockStatic(YstreamClientBoot.class)) {
            factory.when(() -> YstreamClientBoot.<YStreamRecord> getClient()).thenReturn(client);
            assertThatThrownBy(() -> createEventSource().execute(context, partition, offsetContext))
                    .isInstanceOf(InterruptedException.class);
        }

        verify(client).next();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private YStreamStreamingChangeEventSource createEventSource() {
        return new YStreamStreamingChangeEventSource(
                connectorConfig,
                jdbcConnection,
                (EventDispatcher) dispatcher,
                errorHandler,
                clock,
                schema,
                streamingMetrics);
    }
}
