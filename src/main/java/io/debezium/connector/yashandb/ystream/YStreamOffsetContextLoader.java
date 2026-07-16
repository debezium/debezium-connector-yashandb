/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.yashandb.YashanDbConnectorConfig;
import io.debezium.connector.yashandb.YashanDbOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

/**
 * The {@link OffsetContext} loader implementation for the YashanDB YStream adapter.
 */
public class YStreamOffsetContextLoader implements OffsetContext.Loader<YashanDbOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(YStreamOffsetContextLoader.class);
    private final YashanDbConnectorConfig connectorConfig;

    /**
     * Creates a YStreamOffsetContextLoader with the given connector configuration.
     *
     * @param connectorConfig the YashanDB connector configuration
     */
    public YStreamOffsetContextLoader(YashanDbConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    /**
     * Loads a YashanDbOffsetContext from the given offset map.
     *
     * @param offset the offset map containing snapshot and position data
     * @return the loaded YashanDbOffsetContext
     */
    @Override
    public YashanDbOffsetContext load(Map<String, ?> offset) {
        return YashanDbOffsetContext.create()
                .logicalName(connectorConfig)
                .snapshotScn(YashanDbOffsetContext.loadSnapshotScn(offset))
                .recoverPosition(YashanDbOffsetContext.loadRecoverPosition(offset))
                .snapshotPendingTransactions(YashanDbOffsetContext.loadSnapshotPendingTransactions(offset))
                .snapshot(loadSnapshot(offset).orElse(null))
                .snapshotCompleted(loadSnapshotCompleted(offset))
                .transactionContext(TransactionContext.load(offset))
                .incrementalSnapshotContext(SignalBasedIncrementalSnapshotContext.load(offset, false))
                .build();
    }
}
