/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.PhysicalRowIdentifierChunkQueryBuilder;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;
import java.sql.Types;

/**
 * YashanDB implementation that exposes {@code ROWID} as a physical row identifier for incremental snapshots.
 */
public class YashanDBPhysicalRowIdentifierChunkQueryBuilder<T extends DataCollectionId>
        extends PhysicalRowIdentifierChunkQueryBuilder<T> {

    private static final String ROWID = "ROWID";
    private static final String ROWID_TABLE_ALIAS = "DBZ_ROWID_ALIAS";

    public YashanDBPhysicalRowIdentifierChunkQueryBuilder(RelationalDatabaseConnectorConfig config,
                                                          JdbcConnection jdbcConnection) {
        super(config,
                jdbcConnection,
                ROWID,
                ROWID,
                Types.ROWID,
                Types.ROWID,
                ROWID,
                null,
                null,
                true,
                ROWID_TABLE_ALIAS);
    }
}
