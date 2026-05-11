/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.DefaultQueueProvider;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.document.DocumentReader;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

public class YashanDBConnectorTask extends BaseSourceTask<YashanDBPartition, YashanDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDBConnectorTask.class);
    private static final String CONTEXT_NAME = "yashandb-connector-task";

    private final ReentrantLock commitLock = new ReentrantLock();

    private volatile YashanDBTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile YashanDBConnection jdbcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile YashanDBDatabaseSchema schema;
    private YashanDBConnectorConfig connectorConfig;
    private volatile YashanDBConnection beanRegistryJdbcConnection;
    private Partition.Provider<YashanDBPartition> partitionProvider = null;
    private OffsetContext.Loader<YashanDBOffsetContext> offsetContextLoader = null;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration config) {

        connectorConfig = new YashanDBConnectorConfig(config);
        taskContext = new YashanDBTaskContext(config, connectorConfig);

        return taskContext;
    }

    @Override
    public ChangeEventSourceCoordinator<YashanDBPartition, YashanDBOffsetContext> start(Configuration config) {
        partitionProvider = new YashanDBPartition.Provider(connectorConfig);
        offsetContextLoader = connectorConfig.getAdapter().getOffsetContextLoader();
        TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        JdbcConfiguration jdbcConfig = connectorConfig.getJdbcConfig();

        MainConnectionProvidingConnectionFactory<YashanDBConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new YashanDBConnection(jdbcConfig));

        jdbcConnection = connectionFactory.mainConnection();
        validateYStreamServer(connectorConfig);

        validateRedoLogConfiguration(connectorConfig);

        registerServiceProviders(connectorConfig.getServiceRegistry());

        YashanDBValueConverters valueConverters = new YashanDBValueConverters(connectorConfig, jdbcConnection);
        YashanDBDefaultValueConverter defaultValueConverter = new YashanDBDefaultValueConverter(valueConverters, jdbcConnection);
        StreamingAdapter.TableNameCaseSensitivity tableNameCaseSensitivity = connectorConfig.getAdapter().getTableNameCaseSensitivity(jdbcConnection);
        CustomConverterRegistry customConverterRegistry = connectorConfig.getServiceRegistry().tryGetService(CustomConverterRegistry.class);
        this.schema = new YashanDBDatabaseSchema(connectorConfig, valueConverters, defaultValueConverter, schemaNameAdjuster,
                topicNamingStrategy, tableNameCaseSensitivity, customConverterRegistry, taskContext);
        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        Offsets<YashanDBPartition, YashanDBOffsetContext> previousOffsets = getPreviousOffsets(new YashanDBPartition.Provider(connectorConfig),
                connectorConfig.getAdapter().getOffsetContextLoader());

        beanRegistryJdbcConnection = connectionFactory.newConnection();
        // Manual Bean Registration
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, beanRegistryJdbcConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverters);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);

        YashanDBPartition partition = previousOffsets.getTheOnlyPartition();
        YashanDBOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        try {
            validateAndLoadSchemaHistory(connectorConfig, partition, previousOffset, schema);
        }
        catch (InterruptedException e) {
            throw new DebeziumException(e);
        }

        Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .queueProvider(new DefaultQueueProvider<>(connectorConfig.getMaxQueueSize()))
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new YashanDBErrorHandler(connectorConfig, queue, errorHandler);

        final YashanDBEventMetadataProvider metadataProvider = new YashanDBEventMetadataProvider();

        SignalProcessor<YashanDBPartition, YashanDBOffsetContext> signalProcessor = new SignalProcessor<>(
                YashanDBConnector.class, connectorConfig, Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsets);

        EventDispatcher<YashanDBPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                new HeartbeatFactory<>().getScheduledHeartbeat(
                        connectorConfig,
                        () -> getHeartbeatConnection(connectorConfig, jdbcConfig),
                        exception -> {
                            final String sqlErrorId = exception.getMessage();
                            if (exception.getErrorCode() == 2396) {
                                // ORA-02396 idle time expired
                                return;
                            }
                            throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                        }, queue),
                schemaNameAdjuster,
                signalProcessor,
                connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

        final YashanDBStreamingChangeEventSourceMetrics streamingMetrics = new YashanDBStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider,
                connectorConfig, schema::dataCollectionIds);

        NotificationService<YashanDBPartition, YashanDBOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        ChangeEventSourceCoordinator<YashanDBPartition, YashanDBOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                YashanDBConnector.class,
                connectorConfig,
                new YashanDBChangeEventSourceFactory(connectorConfig, connectionFactory, errorHandler, dispatcher, clock, schema, jdbcConfig, taskContext,
                        streamingMetrics, snapshotterService),
                new YashanDBChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema, signalProcessor,
                notificationService, snapshotterService);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    private YashanDBConnection getHeartbeatConnection(YashanDBConnectorConfig connectorConfig, JdbcConfiguration jdbcConfig) {
        return new YashanDBConnection(jdbcConfig);
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();

        return records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    @Override
    public void doStop() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        try {
            if (beanRegistryJdbcConnection != null) {
                beanRegistryJdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC bean registry connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return YashanDBConnectorConfig.ALL_FIELDS;
    }

    @Override
    public void performCommit() {

        final boolean locked = commitLock.tryLock();
        if (!locked) {
            LOGGER.warn("Couldn't commit processed log positions with the source database due to a concurrent connector shutdown or restart");
            return;
        }

        try {
            final Offsets<YashanDBPartition, YashanDBOffsetContext> offsets = getPreviousOffsets(partitionProvider, offsetContextLoader);
            if (offsets.getOffsets() != null) {
                offsets.getOffsets().entrySet().stream()
                        .filter(e -> e.getValue() != null)
                        .max(Comparator.comparing(e -> e.getValue().getLcrPosition()))
                        .ifPresent(entry -> {
                            final Map<String, String> maxPartition = entry.getKey().getSourcePartition();
                            final Map<String, ?> maxOffset = entry.getValue().getOffset();

                            LOGGER.debug("Committing LCR offset position '{}'", maxOffset);
                            coordinator.commitOffset(maxPartition, maxOffset);
                        });
            }
        }
        finally {
            commitLock.unlock();
        }
    }

    private void validateYStreamServer(YashanDBConnectorConfig config) {
        try (Statement statement = jdbcConnection.connection().createStatement()) {
            ResultSet resultSet = statement.executeQuery(
                    String.format("select SERVER_ID,SERVER_NAME,STATUS from SYS.V_$YSTREAM_SERVER where SERVER_NAME = '%s'", config.getYstreamServerName()));
            if (resultSet.next()) {
                String status = resultSet.getString(3);
                if (!(Objects.equals(status, "RUNNING") || Objects.equals(status, "STARTED"))) {
                    throw new DebeziumException("YashanDB YStream server status is " + status + ". Please execute 'DBMS_YSTREAM_ADM.START(\n" +
                            "    server_name   IN  VARCHAR(64)\n" +
                            ");\n' start YStream server");
                }
            }
            else {
                throw new DebeziumException("YashanDB does not yet have the YStream server " + config.getYstreamServerName()
                        + " or check option 'database.ystream.server.name' if the parameters are filled in correctly." +
                        " Please create and configure the YStream server, refer to the link 'https://doc.yashandb.com/yashandb/23.3/zh/%E5%BC%80%E5%8F%91%E6%89%8B%E5%86%8C/PL%E5%8F%82%E8%80%83%E6%89%8B%E5%86%8C/%E5%86%85%E7%BD%AE%E9%AB%98%E7%BA%A7%E5%8C%85/DBMS_YSTREAM_ADM.html'.");
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Query 'select SERVER_ID,SERVER_NAME,STATUS from SYS.V_$YSTREAM_SERVE' fail, please check database status or user Permissions",
                    e);
        }
    }

    private void validateRedoLogConfiguration(YashanDBConnectorConfig config) {
        // Check whether the archive log is enabled.
        final boolean archivelogMode = jdbcConnection.isArchiveLogMode();
        if (!archivelogMode) {
            if (redoLogRequired(config)) {
                throw new DebeziumException("The YashanDB server is not configured to use a archive log LOG_MODE, which is "
                        + "required for this connector to work properly. Change the YashanDB configuration to use a "
                        + "LOG_MODE=ARCHIVELOG and restart the connector.");
            }
            else {
                LOGGER.warn("Failed the archive log check but continuing as redo log isn't strictly required");
            }
        }
    }

    private static boolean redoLogRequired(YashanDBConnectorConfig config) {
        // Check whether our connector configuration relies on the redo log and should fail fast if it isn't configured
        return config.getSnapshotMode().shouldStream() ||
                config.getLogMiningTransactionSnapshotBoundaryMode() == YashanDBConnectorConfig.TransactionSnapshotBoundaryMode.ALL;
    }

    private void validateAndLoadSchemaHistory(YashanDBConnectorConfig config, YashanDBPartition partition, YashanDBOffsetContext offset, YashanDBDatabaseSchema schema)
            throws InterruptedException {
        if (offset == null) {
            if (config.getSnapshotMode().shouldSnapshotOnSchemaError() && config.getSnapshotMode() != YashanDBConnectorConfig.SnapshotMode.ALWAYS) {
                // We are in schema only recovery mode, use the existing redo log position
                // would like to also verify redo log position exists, but it defaults to 0 which is technically valid
                throw new DebeziumException("Could not find existing redo log information while attempting schema only recovery snapshot");
            }
            LOGGER.info("Connector started for the first time, database schema history recovery will not be executed");
            schema.initializeStorage();
            return;
        }
        if (!schema.historyExists()) {
            LOGGER.warn("Database schema history was not found but was expected");
            if (config.getSnapshotMode().shouldSnapshotOnSchemaError()) {
                LOGGER.info("The db-history topic is missing but we are in {} snapshot mode. " +
                        "Attempting to snapshot the current schema and then begin reading the redo log from the last recorded offset.",
                        YashanDBConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY);
            }
            else {
                throw new DebeziumException("The db history topic is missing. You may attempt to recover it by reconfiguring the connector to "
                        + YashanDBConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY);
            }
            schema.initializeStorage();
            return;
        }
        schema.recover(Offsets.of(partition, offset));
    }
}
