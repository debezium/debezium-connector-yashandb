/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yashandb.jdbc.YasTypes;

import io.debezium.DebeziumException;
import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.ChunkQueryBuilder;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * JDBC connection for YashanDB.
 */
public class YashanDbConnection extends JdbcConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDbConnection.class);

    private static final String QUOTED_CHARACTER = "\"";

    private static final Field URL = Field.create("url", "Raw JDBC url");

    private static final int YASHANDB_UNSET_SCALE = -127;

    /**
     * Creates a new YashanDB connection.
     *
     * @param config the JDBC configuration
     */
    public YashanDbConnection(JdbcConfiguration config) {
        super(config, resolveConnectionFactory(config), QUOTED_CHARACTER, QUOTED_CHARACTER);
    }

    /**
     * Creates a new YashanDB connection with a custom connection factory.
     *
     * @param config the JDBC configuration
     * @param connectionFactory the connection factory
     */
    public YashanDbConnection(JdbcConfiguration config, ConnectionFactory connectionFactory) {
        super(config, connectionFactory, QUOTED_CHARACTER, QUOTED_CHARACTER);
    }

    /**
     * Creates a new YashanDB connection with initial operations.
     *
     * @param config the JDBC configuration
     * @param connectionFactory the connection factory
     * @param initialOperations the initial operations to execute
     */
    protected YashanDbConnection(JdbcConfiguration config, ConnectionFactory connectionFactory, Operations initialOperations) {
        super(config, connectionFactory, initialOperations, QUOTED_CHARACTER, QUOTED_CHARACTER);
    }

    /**
     * Retrieves the current system change number from the database.
     *
     * @return the current SCN
     * @throws SQLException if the query fails
     */
    public Scn getCurrentScn() throws SQLException {
        return queryAndMap("SELECT CURRENT_SCN FROM V$DATABASE", (rs) -> {
            if (rs.next()) {
                return Scn.valueOf(rs.getString(1));
            }
            throw new IllegalStateException("Could not get SCN");
        });
    }

    /**
     * Queries the oldest transaction start SCN from open transactions.
     *
     * @return the oldest transaction start SCN, or NULL if no open transactions
     * @throws SQLException if the query fails
     */
    public Scn queryOldTransactionStartScn() throws SQLException {
        return queryAndMap("SELECT MIN(START_SCN) FROM SYS.V_$TRANSACTION WHERE STATUS = 'OPEN'", (rs) -> {
            if (rs.next()) {
                return Scn.valueOf(rs.getLong(1));
            }
            else {
                return Scn.NULL;
            }
        });
    }

    private static ConnectionFactory resolveConnectionFactory(JdbcConfiguration config) {
        return JdbcConnection.patternBasedFactory(connectionString(config));
    }

    /**
     * Builds the JDBC connection string from the configuration.
     *
     * @param config the JDBC configuration
     * @return the connection string
     */
    public static String connectionString(JdbcConfiguration config) {
        return config.getString(URL) != null ? config.getString(URL)
                : String.format("jdbc:yasdb://%s:%s/%s", config.getString(JdbcConfiguration.HOSTNAME), config.getString(JdbcConfiguration.PORT),
                        config.getString(JdbcConfiguration.DATABASE));
    }

    /**
     * Retrieves the DDL metadata for the specified table.
     *
     * @param tableId the table identifier
     * @return the DDL statement for the table
     * @throws SQLException if the query fails
     */
    public String getTableMetadataDdl(TableId tableId) throws SQLException {
        final String schema = tableId.schema();
        final String table = tableId.table();
        final String fqtn = schema + "." + table;
        try {
            final String sql = "SELECT dbms_metadata.get_ddl('TABLE','" + table + "','" + schema + "') FROM DUAL";
            return queryAndMap(sql, rs -> {
                if (!rs.next()) {
                    throw new DebeziumException("Could not get DDL metadata for table: " + fqtn);
                }
                return rs.getString(1);
            });
        }
        catch (SQLException e) {
            throw new SQLException("Failed to get table DDL via dbms_metadata.get_ddl('TABLE') for " + fqtn, e);
        }
    }

    @Override
    public String buildSelectWithRowLimits(TableId tableId,
                                           int limit,
                                           String projection,
                                           Optional<String> condition,
                                           Optional<String> additionalCondition,
                                           String orderBy) {
        final TableId table = new TableId(null, tableId.schema(), tableId.table());
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql
                .append(projection)
                .append(" FROM ");
        sql.append(quotedTableIdString(table));
        if (condition.isPresent()) {
            sql
                    .append(" WHERE ")
                    .append(condition.get());
            if (additionalCondition.isPresent()) {
                sql.append(" AND ");
                sql.append(additionalCondition.get());
            }
        }
        else if (additionalCondition.isPresent()) {
            sql.append(" WHERE ");
            sql.append(additionalCondition.get());
        }
        sql
                .insert(0, " SELECT * FROM (")
                .append(" ORDER BY ")
                .append(orderBy)
                .append(")")
                .append(" WHERE ROWNUM <=")
                .append(limit);
        return sql.toString();
    }

    protected boolean isArchiveLogMode() {
        try {
            final String mode = queryAndMap("SELECT LOG_MODE FROM V$DATABASE", rs -> rs.next() ? rs.getString(1) : "");
            LOGGER.debug("LOG_MODE={}", mode);
            return "ARCHIVELOG".equalsIgnoreCase(mode);
        }
        catch (SQLException e) {
            throw new DebeziumException("Unexpected error while connecting to YashanDB and looking at LOG_MODE mode: ", e);
        }
    }

    /**
     * Retrieves all table IDs from the specified catalog.
     *
     * @param catalogName the catalog name
     * @return the set of table IDs
     * @throws SQLException if the query fails
     */
    public Set<TableId> getAllTableIds(String catalogName) throws SQLException {
        final String query = "select owner, table_name from all_tables ";

        Set<TableId> tableIds = new HashSet<>();
        query(query, (rs) -> {
            while (rs.next()) {
                tableIds.add(new TableId("", rs.getString(1), rs.getString(2)));
            }
            LOGGER.trace("TableIds are: {}", tableIds);
        });

        return tableIds;
    }

    /**
     * Converts an SCN to a timestamp.
     *
     * @param scn the system change number
     * @return the corresponding timestamp, or empty if conversion fails
     * @throws SQLException if the query fails
     */
    public Optional<Instant> getScnToTimestamp(Scn scn) throws SQLException {
        try {
            return queryAndMap("SELECT scn_to_timestamp(" + scn + ") FROM DUAL", rs -> rs.next()
                    ? Optional.of(rs.getTimestamp(1).toInstant())
                    : Optional.empty());
        }
        catch (SQLException e) {
            // Any other SQLException should be thrown
            throw e;
        }
    }

    @Override
    public Optional<Instant> getCurrentTimestamp() throws SQLException {
        return queryAndMap("SELECT CURRENT_TIMESTAMP FROM DUAL",
                rs -> rs.next() ? Optional.of(rs.getTimestamp(1).toInstant()) : Optional.empty());
    }

    @Override
    protected ColumnEditor overrideColumn(ColumnEditor column) {
        // This allows the column state to be overridden before default-value resolution so that the
        // output of the default value is within the same precision as that of the column values.
        if (YasTypes.NUMBER == column.jdbcType() || YasTypes.DECIMAL == column.jdbcType()) {
            column.scale().filter(s -> s == YASHANDB_UNSET_SCALE).ifPresent(s -> column.scale(null));
        }
        return column;
    }

    /**
     * Retrieves the SQL keywords from the JDBC driver metadata.
     *
     * @return the list of SQL keywords
     */
    public List<String> getSQLKeywords() {
        try {
            return Arrays.asList(connection().getMetaData().getSQLKeywords().split(","));
        }
        catch (SQLException e) {
            LOGGER.debug("Failed to acquire SQL keywords from JDBC driver.", e);
            return Collections.emptyList();
        }
    }

    @Override
    public <T extends DataCollectionId> ChunkQueryBuilder<T> chunkQueryBuilder(RelationalDatabaseConnectorConfig connectorConfig) {
        return new YashanDbPhysicalRowIdentifierChunkQueryBuilder<>(connectorConfig, this);
    }
}
