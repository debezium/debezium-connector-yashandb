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

/**
 * Factory for creating YashanDB change event sources.
 */
public class YashanDbChangeEventSourceFactory implements ChangeEventSourceFactory<YashanDbPartition, YashanDbOffsetContext> {

    private final YashanDbConnectorConfig configuration;
    private final MainConnectionProvidingConnectionFactory<YashanDbConnection> connectionFactory;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<YashanDbPartition, TableId> dispatcher;
    private final Clock clock;
    private final YashanDbDatabaseSchema schema;
    private final Configuration jdbcConfig;
    private final YashanDbTaskContext taskContext;
    private final YashanDbStreamingChangeEventSourceMetrics streamingMetrics;
    private final SnapshotterService snapshotterService;

    /**
     * Creates a new change event source factory.
     *
     * @param configuration the connector configuration
     * @param connectionFactory the connection factory
     * @param errorHandler the error handler
     * @param dispatcher the event dispatcher
     * @param clock the clock for time-based operations
     * @param schema the database schema
     * @param jdbcConfig the JDBC configuration
     * @param taskContext the task context
     * @param streamingMetrics the streaming metrics
     * @param snapshotterService the snapshotter service
     */
    public YashanDbChangeEventSourceFactory(YashanDbConnectorConfig configuration, MainConnectionProvidingConnectionFactory<YashanDbConnection> connectionFactory,
                                            ErrorHandler errorHandler, EventDispatcher<YashanDbPartition, TableId> dispatcher, Clock clock, YashanDbDatabaseSchema schema,
                                            Configuration jdbcConfig, YashanDbTaskContext taskContext,
                                            YashanDbStreamingChangeEventSourceMetrics streamingMetrics, SnapshotterService snapshotterService) {
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
    /** {@inheritDoc} */
    public SnapshotChangeEventSource<YashanDbPartition, YashanDbOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<YashanDbPartition> snapshotProgressListener,
                                                                                                            NotificationService<YashanDbPartition, YashanDbOffsetContext> notificationService) {
        return new YashanDbSnapshotChangeEventSource(configuration, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService,
                snapshotterService);
    }

    @Override
    /** {@inheritDoc} */
    public StreamingChangeEventSource<YashanDbPartition, YashanDbOffsetContext> getStreamingChangeEventSource() {
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
    /** {@inheritDoc} */
    public Optional<IncrementalSnapshotChangeEventSource<YashanDbPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                                 YashanDbOffsetContext offsetContext,
                                                                                                                                                 SnapshotProgressListener<YashanDbPartition> snapshotProgressListener,
                                                                                                                                                 DataChangeEventListener<YashanDbPartition> dataChangeEventListener,
                                                                                                                                                 NotificationService<YashanDbPartition, YashanDbOffsetContext> notificationService) {
        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        if (configuration.getSignalingDataCollectionIds().isEmpty()) {
            return Optional.empty();
        }

        // Incremental snapshots requires a secondary database connection
        // This is because Xstream does not allow any work on the connection while the LCR handler may be invoked
        // and LogMiner streams results from the CDB$ROOT container but we will need to stream changes from the
        // PDB when reading snapshot records.
        return Optional.of(new YashanDbSignalBasedIncrementalSnapshotChangeEventSource(
                configuration,
                new YashanDbConnection(connectionFactory.mainConnection().config()),
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService));
    }
}
