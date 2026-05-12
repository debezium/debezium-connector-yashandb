package io.debezium.connector.yashandb;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import io.debezium.pipeline.source.snapshot.incremental.ChunkQueryBuilder;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yashandb.jdbc.YasTypes;

import io.debezium.DebeziumException;
import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;

public class YashanDBConnection extends JdbcConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDBConnection.class);

    private static final String QUOTED_CHARACTER = "\"";

    private static final Field URL = Field.create("url", "Raw JDBC url");

    private static final int YASHANDB_UNSET_SCALE = -127;

    public YashanDBConnection(JdbcConfiguration config) {
        super(config, resolveConnectionFactory(config), QUOTED_CHARACTER, QUOTED_CHARACTER);
    }

    public YashanDBConnection(JdbcConfiguration config, ConnectionFactory connectionFactory) {
        super(config, connectionFactory, QUOTED_CHARACTER, QUOTED_CHARACTER);
    }

    protected YashanDBConnection(JdbcConfiguration config, ConnectionFactory connectionFactory, Operations initialOperations) {
        super(config, connectionFactory, initialOperations, QUOTED_CHARACTER, QUOTED_CHARACTER);
    }

    public Scn getCurrentScn() throws SQLException {
        return queryAndMap("SELECT CURRENT_SCN FROM V$DATABASE", (rs) -> {
            if (rs.next()) {
                return Scn.valueOf(rs.getString(1));
            }
            throw new IllegalStateException("Could not get SCN");
        });
    }

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

    public static String connectionString(JdbcConfiguration config) {
        return config.getString(URL) != null ? config.getString(URL)
                : String.format("jdbc:yasdb://%s:%s/%s", config.getString(JdbcConfiguration.HOSTNAME), config.getString(JdbcConfiguration.PORT),
                        config.getString(JdbcConfiguration.DATABASE));
    }

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
        return new YashanDBPhysicalRowIdentifierChunkQueryBuilder<>(connectorConfig, this);
    }
}
