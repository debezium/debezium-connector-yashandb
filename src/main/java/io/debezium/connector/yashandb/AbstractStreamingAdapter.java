/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;

/**
 * Abstract implementation of the {@link StreamingAdapter} for which all streaming adapters are derived.
 */
public abstract class AbstractStreamingAdapter implements StreamingAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStreamingAdapter.class);

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

        StringBuilder lastDdlScnQuery = new StringBuilder("SELECT TIMESTAMP_TO_SCN(MAX(to_timestamp(last_ddl_time)))")
                .append(" FROM all_objects")
                .append(" WHERE");

        for (TableId table : ctx.capturedTables) {
            lastDdlScnQuery.append(" (owner = ? AND object_name = ?) OR");
        }

        String query = lastDdlScnQuery.substring(0, lastDdlScnQuery.length() - 3);
        try (PreparedStatement stmt = connection.connection().prepareStatement(query)) {
            int paramIndex = 1;
            for (TableId table : ctx.capturedTables) {
                stmt.setString(paramIndex++, table.schema());
                stmt.setString(paramIndex++, table.table());
            }
            ResultSet rs = stmt.executeQuery();

            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get latest table DDL SCN");
            }

            // Guard against LAST_DDL_TIME with value of 0.
            // This case should be treated as if we were unable to determine a value for LAST_DDL_TIME.
            // This forces later calculations to be based upon the current SCN.
            String latestDdlTime = rs.getString(1);
            if ("0".equals(latestDdlTime)) {
                return Optional.empty();
            }

            return Optional.of(Scn.valueOf(latestDdlTime));
        }
    }
}
