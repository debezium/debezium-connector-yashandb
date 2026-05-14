/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.yashandb.ystream.YStreamAdapter;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;

/**
 * Connector configuration for YashanDB.
 */
public class YashanDbConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    protected static final int DEFAULT_PORT = 1688;

    protected static final int DEFAULT_QUERY_FETCH_SIZE = 10_000;

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
            .withDefault(DEFAULT_PORT);

    public static final Field HOSTNAME = RelationalDatabaseConnectorConfig.HOSTNAME
            .withNoValidation()
            .withValidation(YashanDbConnectorConfig::requiredWhenNoUrl);

    public static final Field INTERVAL_HANDLING_MODE = Field.create("interval.handling.mode")
            .withDisplayName("Interval Handling")
            .withEnum(IntervalHandlingMode.class, IntervalHandlingMode.NUMERIC)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 6))
            .withDescription("Specify how INTERVAL columns should be represented in change events, including: "
                    + "'string' represents values as an exact ISO formatted string; "
                    + "'numeric' (default) represents values using the inexact conversion into microseconds");

    public static final Field DDL_PARSE_FAIL_RETRY_READ_TABLE = Field.create("ddl.parse.fail.retry.read.table")
            .withDisplayName("Ddl parse error handling mode")
            .withType(Type.BOOLEAN)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 6))
            .withDescription("Incremental DDL Parsing Failure Handling Mode,including:" +
                    "false -> Do nothing and throw an exception;" +
                    "true -> Wait for the next DML event and read the source table DDL")
            .withDefault(false);

    public static final Field YSTREAM_SERVER_NAME = Field.create(DATABASE_CONFIG_PREFIX + "ystream.server.name")
            .withDisplayName("Ystream out server name")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 9))
            // .withValidation(YashanDbConnectorConfig::validateOutServerName)
            .withDescription("Name of the Ystream Out server to connect to.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Select one of the following snapshot options: "
                    + "'always': The connector runs a snapshot every time that it starts. After the snapshot completes, the connector begins to stream changes from the redo logs.; "
                    + "'initial' (default): If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures the current full state of the configured tables. After the snapshot completes, the connector begins to stream changes from the redo logs. "
                    + "'initial_only': The connector performs a snapshot as it does for the 'initial' option, but after the connector completes the snapshot, it stops, and does not stream changes from the redo logs.; "
                    + "'schema_only': If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures only the schema (table structures), but not any table data. After the snapshot completes, the connector begins to stream changes from the redo logs.; "
                    + "'schema_only_recovery': The connector performs a snapshot that captures only the database schema history. The connector then transitions to streaming from the redo logs. Use this setting to restore a corrupted or lost database schema history topic. Do not use if the database schema was modified after the connector stopped.");

    public static final Field SNAPSHOT_LOCKING_MODE = Field.create("snapshot.locking.mode")
            .withDisplayName("Snapshot locking mode")
            .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.SHARED)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 1))
            .withDescription("Controls how the connector holds locks on tables while performing the schema snapshot. The default is 'shared', "
                    + "which means the connector will hold a table lock that prevents exclusive table access for just the initial portion of the snapshot "
                    + "while the database schemas and other metadata are being read. The remaining work in a snapshot involves selecting all rows from "
                    + "each table, and this is done using a flashback query that requires no locks. However, in some cases it may be desirable to avoid "
                    + "locks entirely which can be done by specifying 'none'. This mode is only safe to use if no schema changes are happening while the "
                    + "snapshot is taken.");

    public static final Field SNAPSHOT_ENHANCEMENT_TOKEN = Field.create("snapshot.enhance.predicate.scn")
            .withDisplayName("A string to replace on snapshot predicate enhancement")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 11))
            .withDescription("A token to replace on snapshot predicate template");

    public static final Field SNAPSHOT_DATABASE_ERRORS_MAX_RETRIES = Field.create("snapshot.database.errors.max.retries")
            .withDisplayName("The maximum number of retries before snapshot database errors are not retried")
            .withType(Type.INT)
            .withDefault(0)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withValidation(Field::isNonNegativeInteger)
            .withDescription("The number of attempts to retry database errors during snapshots before failing.");

    public static final Field URL = Field.create(DATABASE_CONFIG_PREFIX + "url")
            .withDisplayName("Complete JDBC URL")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(YashanDbConnectorConfig::requiredWhenNoHostname)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 10))
            .withDescription("Complete JDBC URL as an alternative to specifying hostname, port and database provided "
                    + "as a way to support alternative connection scenarios.");

    public static final Field LOB_ENABLED = Field.create("lob.enabled")
            .withDisplayName("Specifies whether the connector supports mining LOB fields and operations")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 21))
            .withDefault(false)
            .withDescription("When set to 'false', the default, LOB fields will not be captured nor emitted. When set to 'true', the connector " +
                    "will capture LOB fields and emit changes for those fields like any other column type.");

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(YashanDbSourceInfoStructMaker.class.getName());

    public static final Field QUERY_FETCH_SIZE = CommonConnectorConfig.QUERY_FETCH_SIZE
            .withDescription(
                    "The maximum number of records that should be loaded into memory while streaming. A value of '0' uses the default JDBC fetch size, defaults to '2000'.")
            .withDefault(DEFAULT_QUERY_FETCH_SIZE);

    public static final Field LEGACY_DECIMAL_HANDLING_STRATEGY = Field.create("legacy.decimal.handling.strategy")
            .withDisplayName("Use legacy decimal handling strategy")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription("Uses the legacy decimal handling behavior before DBZ-7882");

    // ------------------------------------------- ystream options
    // ------------------------------------------------------
    // Ystream options refer to:
    // http://doc.yashandb.com/yashandb/23.4/zh/All-Manuals/Development-Guide/YStream-Reference-Manual/YStream-Client-Usage-Introduction.html

    public static final Field YSTREAM_QUEUE_SIZE = Field.create("ystream.blocking.queue.size")
            .withDisplayName("YashanDB YStream blocking queue size")
            .withType(Type.INT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(128)
            .withDescription(
                    "The length of the YStream client's built-in "
                            + "blocking queue is directly obtained from this queue when retrieving incremental logical logs,"
                            + " with a default value of 128.");

    public static final Field YSTREAM_POLL_TIMEOUT = Field.create("ystream.poll.timeout")
            .withType(Type.INT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(10)
            .withDescription(
                    "The timeout (in seconds) for obtaining the next result from the blocking queue,"
                            + " with a default value of 10.");

    public static final Field YSTREAM_CLIENT_RESPONSE_TIMEOUT = Field.create("ystream.client.response.timeout")
            .withType(Type.INT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(60)
            .withDescription(
                    "The maximum time (in seconds) that the YStream server can wait for a response from the YStream client,"
                            + " with a default value of 60.");

    public static final Field LOGIC_SHARD_ENABLED = Field.create("logic.shard.enabled")
            .withType(Type.BOOLEAN)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription(
                    "Whether to enable logical sharding,default enabled.");

    public static final Field TABLE_READ_THREADS = Field.create("table.read.threads")
            .withType(Type.INT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(1)
            .withDescription(
                    "Number of read threads per table.Default 1 thread.");

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("YashanDB")
            .excluding(
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_EXCLUDE_LIST,
                    RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
                    CommonConnectorConfig.QUERY_FETCH_SIZE)
            .type(
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    DATABASE_NAME,
                    YSTREAM_SERVER_NAME,
                    SNAPSHOT_MODE,
                    URL)
            .connector(
                    QUERY_FETCH_SIZE,
                    SNAPSHOT_ENHANCEMENT_TOKEN,
                    SNAPSHOT_LOCKING_MODE,
                    INTERVAL_HANDLING_MODE,
                    YSTREAM_CLIENT_RESPONSE_TIMEOUT,
                    YSTREAM_POLL_TIMEOUT,
                    YSTREAM_QUEUE_SIZE,
                    LOGIC_SHARD_ENABLED,
                    TABLE_READ_THREADS,
                    LOB_ENABLED,
                    UNAVAILABLE_VALUE_PLACEHOLDER,
                    BINARY_HANDLING_MODE,
                    SNAPSHOT_DATABASE_ERRORS_MAX_RETRIES,
                    SCHEMA_NAME_ADJUSTMENT_MODE,
                    LEGACY_DECIMAL_HANDLING_STRATEGY)
            .events(SOURCE_INFO_STRUCT_MAKER)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    /**
     * Returns the configuration definition.
     *
     * @return the ConfigDef
     */
    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    public static final List<String> EXCLUDED_SCHEMAS = Collections.unmodifiableList(Arrays.asList("SYS", "MDSYS",
            "XA_SYS"));

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDbConnectorConfig.class);

    private final String databaseName;
    private final IntervalHandlingMode intervalHandlingMode;
    private final SnapshotMode snapshotMode;

    private final String snapshotEnhancementToken;
    private final SnapshotLockingMode snapshotLockingMode;
    private final int queryFetchSize;

    private final boolean lobEnabled;
    private final StreamingAdapter streamingAdapter;
    private final String ystreamServerName;
    private final boolean ddlParseFailRetryReadTable;
    private final int yStreamQueueSize;
    private final int yStreamPollTimeout;
    private final int yStreamClientResponseTimeout;
    private final Boolean logicShardEnabled;
    private final int tableReadThreads;
    private final boolean legacyDecimalHandlingStrategy;
    private final int snapshotRetryDatabaseErrorsMaxRetries;

    /**
     * Creates a new connector configuration.
     *
     * @param config the configuration properties
     */
    public YashanDbConnectorConfig(Configuration config) {
        super(
                YashanDbConnector.class, config,
                new SystemTablesPredicate(config),
                x -> x.schema() + "." + x.table(),
                false,
                DEFAULT_QUERY_FETCH_SIZE,
                ColumnFilterMode.SCHEMA,
                false);

        this.databaseName = toUpperCase(config.getString(DATABASE_NAME));
        this.intervalHandlingMode = IntervalHandlingMode.parse(config.getString(INTERVAL_HANDLING_MODE));
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.snapshotEnhancementToken = config.getString(SNAPSHOT_ENHANCEMENT_TOKEN);
        this.snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE), SNAPSHOT_LOCKING_MODE.defaultValueAsString());
        this.lobEnabled = config.getBoolean(LOB_ENABLED);
        this.queryFetchSize = config.getInteger(QUERY_FETCH_SIZE);
        this.streamingAdapter = new YStreamAdapter(this);
        this.ystreamServerName = config.getString(YSTREAM_SERVER_NAME);
        this.ddlParseFailRetryReadTable = config.getBoolean(DDL_PARSE_FAIL_RETRY_READ_TABLE);

        // YStream
        this.yStreamPollTimeout = config.getInteger(YSTREAM_POLL_TIMEOUT);
        this.yStreamQueueSize = config.getInteger(YSTREAM_QUEUE_SIZE);
        this.snapshotRetryDatabaseErrorsMaxRetries = config.getInteger(SNAPSHOT_DATABASE_ERRORS_MAX_RETRIES);
        this.yStreamClientResponseTimeout = config.getInteger(YSTREAM_CLIENT_RESPONSE_TIMEOUT);

        // Shard
        this.logicShardEnabled = config.getBoolean(LOGIC_SHARD_ENABLED);
        this.tableReadThreads = config.getInteger(TABLE_READ_THREADS);

        // Legacy decimal handling
        this.legacyDecimalHandlingStrategy = config.getBoolean(LEGACY_DECIMAL_HANDLING_STRATEGY);
    }

    private static String toUpperCase(String property) {
        return property == null ? null : property.toUpperCase();
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Returns the YStream server name.
     *
     * @return the YStream server name
     */
    public String getYstreamServerName() {
        return ystreamServerName;
    }

    /**
     * Returns whether DDL parse failure retry reading table is enabled.
     *
     * @return true if retry is enabled, false otherwise
     */
    public Boolean getDdlParseFailRetryReadTable() {
        return ddlParseFailRetryReadTable;
    }

    /**
     * Returns the interval handling mode.
     *
     * @return the interval handling mode
     */
    public IntervalHandlingMode getIntervalHandlingMode() {
        return intervalHandlingMode;
    }

    /**
     * Returns the snapshot mode.
     *
     * @return the snapshot mode
     */
    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    /**
     * Returns the snapshot locking mode.
     *
     * @return the snapshot locking mode
     */
    public Optional<SnapshotLockingMode> getSnapshotLockingMode() {
        return Optional.ofNullable(snapshotLockingMode);
    }

    /**
     * Returns the YStream queue size.
     *
     * @return the queue size
     */
    public int getyStreamQueueSize() {
        return yStreamQueueSize;
    }

    /**
     * Returns the YStream poll timeout in seconds.
     *
     * @return the poll timeout
     */
    public int getyStreamPollTimeout() {
        return yStreamPollTimeout;
    }

    /**
     * Returns the YStream client response timeout in seconds.
     *
     * @return the response timeout
     */
    public int getyStreamClientResponseTimeout() {
        return yStreamClientResponseTimeout;
    }

    /**
     * Returns whether logical sharding is enabled.
     *
     * @return true if enabled, false otherwise
     */
    public Boolean getLogicShardEnabled() {
        return logicShardEnabled;
    }

    /**
     * Returns the number of read threads per table.
     *
     * @return the thread count
     */
    public int getTableReadThreads() {
        return tableReadThreads;
    }

    /**
     * Returns the maximum number of retries for snapshot database errors.
     *
     * @return the max retry count
     */
    public int getSnapshotRetryDatabaseErrorsMaxRetries() {
        return snapshotRetryDatabaseErrorsMaxRetries;
    }

    /**
     * Returns whether the legacy decimal handling strategy is used.
     *
     * @return true if legacy strategy is used, false otherwise
     */
    public boolean isUsingLegacyDecimalHandlingStrategy() {
        return legacyDecimalHandlingStrategy;
    }

    @Override
    public int getQueryFetchSize() {
        return queryFetchSize;
    }

    @Override
    public HistoryRecordComparator getHistoryRecordComparator() {
        return streamingAdapter.getHistoryRecordComparator();
    }

    /**
     * Defines modes of representation of {@code interval} datatype
     */
    public enum IntervalHandlingMode implements EnumeratedValue {

        /**
         * Represents interval as inexact microseconds count
         */
        NUMERIC("numeric"),

        /**
         * Represents interval as ISO 8601 time interval
         */
        STRING("string");

        private final String value;

        IntervalHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Convert mode name into the logical value
         *
         * @param value the configuration property value ; may not be null
         * @return the matching option, or null if the match is not found
         */
        public static IntervalHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (IntervalHandlingMode option : IntervalHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Convert mode name into the logical value
         *
         * @param value        the configuration property value ; may not be null
         * @param defaultValue the default value ; may be null
         * @return the matching option or null if the match is not found and non-null default is invalid
         */
        public static IntervalHandlingMode parse(String value, String defaultValue) {
            IntervalHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Performs a snapshot of data and schema upon each connector start.
         */
        ALWAYS("always"),

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector.
         */
        INITIAL("initial"),

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector and stop after initial consistent snapshot.
         */
        INITIAL_ONLY("initial_only"),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         */
        NO_DATA("no_data"),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin reading the redo log at the current redo log position.
         * This can be used for recovery only if the connector has existing offsets and the schema.history.internal.kafka.topic does not exist (deleted).
         * This recovery option should be used with care as it assumes there have been no schema changes since the connector last stopped,
         * otherwise some events during the gap may be processed with an incorrect schema and corrupted.
         */
        RECOVERY("recovery"),

        /**
         * Perform a snapshot when it is needed.
         */
        WHEN_NEEDED("when_needed"),

        /**
         * Allows control over snapshots by setting connectors properties prefixed with 'snapshot.mode.configuration.based'.
         */
        CONFIGURATION_BASED("configuration_based"),

        /**
         * Inject a custom snapshotter, which allows for more control over snapshots.
         */
        CUSTOM("custom");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    public enum SnapshotLockingMode implements EnumeratedValue {
        /**
         * This mode will allow concurrent access to the table during the snapshot but prevents any
         * session from acquiring any table-level exclusive lock.
         */
        SHARED("shared"),

        /**
         * This mode will avoid using ANY table locks during the snapshot process.
         * This mode should be used carefully only when no schema changes are to occur.
         */
        NONE("none");

        private final String value;

        SnapshotLockingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean usesLocking() {
            return !value.equals(NONE.value);
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be {@code null}
         * @return the matching option, or null if no match is found
         */
        public static SnapshotLockingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotLockingMode option : SnapshotLockingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value        the configuration property value; may not be {@code null}
         * @param defaultValue the default value; may be {@code null}
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotLockingMode parse(String value, String defaultValue) {
            SnapshotLockingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * A {@link TableFilter} that excludes all YashanDB system tables.
     */
    private static class SystemTablesPredicate implements TableFilter {

        private final Configuration config;

        SystemTablesPredicate(Configuration config) {
            this.config = config;
        }

        @Override
        public boolean isIncluded(TableId t) {
            // not system schema
            return !isExcludedSchema(t);
        }

        private boolean isExcludedSchema(TableId id) {
            return EXCLUDED_SCHEMAS.contains(id.schema());
        }
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    /**
     * Returns the token to replace in snapshot predicate.
     *
     * @return the replacement token
     */
    public String getTokenToReplaceInSnapshotPredicate() {
        return snapshotEnhancementToken;
    }

    /**
     * Returns the streaming adapter.
     *
     * @return the streaming adapter
     */
    public StreamingAdapter getAdapter() {
        return streamingAdapter;
    }

    /**
     * Returns whether LOB fields are to be captured.
     *
     * @return true if LOB capture is enabled, false otherwise
     */
    public boolean isLobEnabled() {
        return lobEnabled;
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    public static int requiredWhenNoUrl(Configuration config, Field field, ValidationOutput problems) {

        // Validates that the field is required but only when an URL field is not present
        if (config.getString(URL) == null) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    public static int requiredWhenNoHostname(Configuration config, Field field, ValidationOutput problems) {

        // Validates that the field is required but only when an URL field is not present
        if (config.getString(HOSTNAME) == null) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

}
