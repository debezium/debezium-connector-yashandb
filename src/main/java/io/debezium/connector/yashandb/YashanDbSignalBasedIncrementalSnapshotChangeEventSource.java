/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.sql.SQLException;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;

public class YashanDbSignalBasedIncrementalSnapshotChangeEventSource extends SignalBasedIncrementalSnapshotChangeEventSource<YashanDbPartition, TableId> {

    private final YashanDbConnection connection;

    public YashanDbSignalBasedIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                                   JdbcConnection jdbcConnection,
                                                                   EventDispatcher<YashanDbPartition, TableId> dispatcher,
                                                                   DatabaseSchema<?> databaseSchema,
                                                                   Clock clock,
                                                                   SnapshotProgressListener<YashanDbPartition> progressListener,
                                                                   DataChangeEventListener<YashanDbPartition> dataChangeEventListener,
                                                                   NotificationService<YashanDbPartition, YashanDbOffsetContext> notificationService) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        this.connection = (YashanDbConnection) jdbcConnection;
    }

    @Override
    protected String getSignalTableName(String dataCollectionId) {
        final TableId tableId = YashanDbTableIdParser.parse(dataCollectionId);
        return YashanDbTableIdParser.quoteIfNeeded(tableId, false, true, connection.getSQLKeywords());
    }

    @Override
    protected void preReadChunk(IncrementalSnapshotContext<TableId> context) {
        super.preReadChunk(context);
    }

    @Override
    protected void postReadChunk(IncrementalSnapshotContext<TableId> context) {
        super.postReadChunk(context);
    }

    @Override
    protected void postIncrementalSnapshotCompleted() {
        super.postIncrementalSnapshotCompleted();

        try {
            connection.close();
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to close snapshot connection", e);
        }
    }
}
