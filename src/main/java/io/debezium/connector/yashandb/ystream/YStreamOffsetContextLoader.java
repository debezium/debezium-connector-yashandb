/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sics.ystream.result.Position;

import io.debezium.connector.yashandb.Scn;
import io.debezium.connector.yashandb.SourceInfo;
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
        boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
        boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(YashanDbOffsetContext.SNAPSHOT_COMPLETED_KEY));
        boolean isCreateServer = Boolean.TRUE.equals(offset.get(YashanDbOffsetContext.YSTREAM_SERVER_CREATE));
        String lcrPosition = (String) offset.get(SourceInfo.LCR_POSITION_KEY);

        final Scn scn;
        if (lcrPosition != null) {
            scn = YStreamPosition.valueOf(lcrPosition).getScn();
        }
        else {
            scn = YashanDbOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        }

        final Map<String, Scn> snapshotPendingTransactions = YashanDbOffsetContext.loadSnapshotPendingTransactions(offset);
        final Scn snapshotScn = YashanDbOffsetContext.loadSnapshotScn(offset);
        final Scn ystreamStartScn = YashanDbOffsetContext.loadYstreamStartScn(offset);
        final Position recoverPosition = YashanDbOffsetContext.loadRecoverPosition(offset);
        LOGGER.debug("loader offset context isCreateServer:{}, position:{}", isCreateServer, recoverPosition);
        return new YashanDbOffsetContext(connectorConfig, scn, snapshotScn, ystreamStartScn, recoverPosition, snapshotPendingTransactions,
                snapshot, snapshotCompleted, TransactionContext.load(offset), SignalBasedIncrementalSnapshotContext.load(offset), isCreateServer);
    }
}
