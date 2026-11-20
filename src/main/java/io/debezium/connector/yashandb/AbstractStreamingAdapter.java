/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.yashandb.dialect.YashanDbDialect;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;

/**
 * Abstract implementation of the {@link StreamingAdapter} for which all streaming adapters are derived.
 */
public abstract class AbstractStreamingAdapter implements StreamingAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStreamingAdapter.class);
    private static final int TABLE_DDL_SCN_BATCH_SIZE = 1000;

    protected final YashanDbConnectorConfig connectorConfig;

    /**
     * Creates an AbstractStreamingAdapter instance initialized with the given connector configuration.
     *
     * @param connectorConfig the connector configuration, must not be null
     */
    public AbstractStreamingAdapter(YashanDbConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    /**
     * Checks whether the two specified system change numbers have the same timestamp.
     *
     * @param scn1 first scn number, may be {@code null}
     * @param scn2 second scn number, may be {@code null}
     * @param connection the database connection, must not be {@code null}
     * @return true if the two system change numbers have the same timestamp; false otherwise
     * @throws SQLException if a database error occurred
     */
    protected boolean areSameTimestamp(Scn scn1, Scn scn2, YashanDbConnection connection) throws SQLException {
        if (scn1 == null) {
            return false;
        }
        if (scn2 == null) {
            return false;
        }

        final String query = "SELECT 1 FROM DUAL WHERE SCN_TO_TIMESTAMP(?)=SCN_TO_TIMESTAMP(?)";
        try (PreparedStatement ps = connection.connection().prepareStatement(query)) {
            ps.setLong(1, scn1.longValue());
            ps.setLong(2, scn2.longValue());
            ResultSet rs = ps.executeQuery();
            return rs.next();
        }
    }

    /**
     * Returns the SCN of the latest DDL change to the captured tables.
     * The result will be empty if there is no table to capture as per the configuration.
     * @param ctx the snapshot contest, must not be {@code null}
     * @param connection the database connection, must not be {@code null}
     * @return the latest table DDL system change number, never {@code null} but may be empty.
     * @throws SQLException if a database error occurred
     */
    protected Optional<Scn> getLatestTableDdlScn(RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> ctx, YashanDbConnection connection)
            throws SQLException {
        if (ctx.capturedTables.isEmpty()) {
            return Optional.empty();
        }

        YashanDbDialect dialect = connectorConfig.getModeContext().dialect();
        return queryLatestTableDdlScn(ctx.capturedTables, dialect, connection.connection());
    }

    static Optional<Scn> queryLatestTableDdlScn(Collection<TableId> capturedTables, YashanDbDialect dialect, Connection connection)
            throws SQLException {
        List<TableId> tables = new ArrayList<>(capturedTables);
        Scn latestDdlScn = null;
        for (int start = 0; start < tables.size(); start += TABLE_DDL_SCN_BATCH_SIZE) {
            List<TableId> batch = tables.subList(start, Math.min(start + TABLE_DDL_SCN_BATCH_SIZE, tables.size()));
            String query = dialect.latestTableDdlScnQuery(batch);
            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                dialect.bindLatestTableDdlScnQuery(stmt, batch);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException("Couldn't get latest table DDL SCN");
                    }
                    Scn batchScn = Scn.valueOf(rs.getString(1));
                    if (latestDdlScn == null || batchScn.compareTo(latestDdlScn) > 0) {
                        latestDdlScn = batchScn;
                    }
                }
            }
        }

        // A zero SCN means LAST_DDL_TIME could not be determined, so later calculations use the current SCN.
        return latestDdlScn == null || latestDdlScn.equals(Scn.valueOf(0)) ? Optional.empty() : Optional.of(latestDdlScn);
    }
}
