/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.util.Optional;

import io.debezium.config.Configuration;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

public class YashanDBChangeEventSourceFactory implements ChangeEventSourceFactory<YashanDBPartition, YashanDBOffsetContext> {

    private final YashanDBConnectorConfig configuration;
    private final MainConnectionProvidingConnectionFactory<YashanDBConnection> connectionFactory;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<YashanDBPartition, TableId> dispatcher;
    private final Clock clock;
    private final YashanDBDatabaseSchema schema;
    private final Configuration jdbcConfig;
    private final YashanDBTaskContext taskContext;
    private final YashanDBStreamingChangeEventSourceMetrics streamingMetrics;
    private final SnapshotterService snapshotterService;

    public YashanDBChangeEventSourceFactory(YashanDBConnectorConfig configuration, MainConnectionProvidingConnectionFactory<YashanDBConnection> connectionFactory,
                                            ErrorHandler errorHandler, EventDispatcher<YashanDBPartition, TableId> dispatcher, Clock clock, YashanDBDatabaseSchema schema,
                                            Configuration jdbcConfig, YashanDBTaskContext taskContext,
                                            YashanDBStreamingChangeEventSourceMetrics streamingMetrics, SnapshotterService snapshotterService) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.jdbcConfig = jdbcConfig;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public SnapshotChangeEventSource<YashanDBPartition, YashanDBOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<YashanDBPartition> snapshotProgressListener,
                                                                                                            NotificationService<YashanDBPartition, YashanDBOffsetContext> notificationService) {
        return new YashanDBSnapshotChangeEventSource(configuration, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService,
                snapshotterService);
    }

    @Override
    public StreamingChangeEventSource<YashanDBPartition, YashanDBOffsetContext> getStreamingChangeEventSource() {
        return configuration.getAdapter().getSource(
                connectionFactory.mainConnection(),
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                jdbcConfig,
                streamingMetrics);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<YashanDBPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                                 YashanDBOffsetContext offsetContext,
                                                                                                                                                 SnapshotProgressListener<YashanDBPartition> snapshotProgressListener,
                                                                                                                                                 DataChangeEventListener<YashanDBPartition> dataChangeEventListener,
                                                                                                                                                 NotificationService<YashanDBPartition, YashanDBOffsetContext> notificationService) {
        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        if (configuration.getSignalingDataCollectionIds().isEmpty()) {
            return Optional.empty();
        }

        // Incremental snapshots requires a secondary database connection
        // This is because Xstream does not allow any work on the connection while the LCR handler may be invoked
        // and LogMiner streams results from the CDB$ROOT container but we will need to stream changes from the
        // PDB when reading snapshot records.
        return Optional.of(new YashanDBSignalBasedIncrementalSnapshotChangeEventSource(
                configuration,
                new YashanDBConnection(connectionFactory.mainConnection().config()),
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService));
    }
}
