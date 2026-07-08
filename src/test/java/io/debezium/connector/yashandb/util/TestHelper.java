/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.util;

import java.nio.file.Path;
import java.sql.SQLException;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.yashandb.YashanDbConnection;
import io.debezium.connector.yashandb.YashanDbConnectorConfig;
import io.debezium.embedded.async.AsyncEngineConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

/**
 * Shared configuration helpers for YashanDB integration tests.
 */
public final class TestHelper {

    public static final String TEST_DATABASE = "testdb";
    public static final String TEST_CONNECTOR = "yashandb_server";
    public static final String TEST_SCHEMA = "DBZ";
    public static final String TEST_USER = "DBZ";
    public static final String TEST_PASSWORD = "dbz";
    public static final String DEFAULT_YSTREAM_SERVER = "ystream_server";
    private static final int DEFAULT_PORT = 1688;
    public static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();

    private TestHelper() {
    }

    public static JdbcConfiguration.Builder adminJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties(ConfigurationNames.DATABASE_CONFIG_PREFIX))
                .withDefault(JdbcConfiguration.DATABASE, TEST_DATABASE)
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, DEFAULT_PORT)
                .withDefault(JdbcConfiguration.USER, "SYS")
                .withDefault(JdbcConfiguration.PASSWORD, "Cod-2022");
    }

    public static YashanDbConnection connectedConnection() {
        final YashanDbConnection connection = TestHelper.create();
        try {
            connection.connect();
            return connection;
        }
        catch (SQLException e) {
            throw new IllegalStateException("Could not connect to YashanDB", e);
        }
    }

    public static JdbcConfiguration.Builder defaultJdbcConfig() {
        return adminJdbcConfig();
    }

    public static JdbcConfiguration.Builder testJdbcConfig() {
        return JdbcConfiguration.copy(adminJdbcConfig().build())
                .with(JdbcConfiguration.USER, TEST_USER)
                .with(JdbcConfiguration.PASSWORD, TEST_PASSWORD);
    }

    public static Configuration.Builder defaultConfig() {
        return Configuration.copy(testJdbcConfig().build().map(key -> ConfigurationNames.DATABASE_CONFIG_PREFIX + key))
                .with(CommonConnectorConfig.EXECUTOR_SHUTDOWN_TIMEOUT_MS, 28_657)
                .with(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS, 196_418)
                .with(CommonConnectorConfig.TOPIC_PREFIX, TestHelper::server)
                .with(YashanDbConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(YashanDbConnectorConfig.YSTREAM_SERVER_NAME, ystreamServerName())
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .with(YashanDbConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    public static String getTestUser() {
        return TEST_USER;
    }

    public static String ystreamServerName() {
        return System.getProperty("database.ystream.server.name", TestHelper.DEFAULT_YSTREAM_SERVER);
    }

    public static YashanDbConnection adminConnection() {
        return new YashanDbConnection(adminJdbcConfig().build());
    }

    public static YashanDbConnection create() {
        return new YashanDbConnection(testJdbcConfig().build());
    }

    public static void dropTestUser() throws Exception {
        try (YashanDbConnection connection = adminConnection()) {
            connection.connect();
            try {
                connection.execute("DROP USER " + TEST_USER + " cascade");
            }
            catch (Exception e) {
                if (!isDoesNotExistError(e)) {
                    throw e;
                }
            }
        }
    }

    public static void createTestUser() throws Exception {
        try (YashanDbConnection connection = adminConnection()) {
            connection.connect();
            try {
                connection.execute("CREATE USER " + TEST_USER + " IDENTIFIED BY \"" + TEST_PASSWORD + "\"");
            }
            catch (Exception e) {
                if (!isAlreadyExistsError(e)) {
                    throw e;
                }
            }
            connection.execute("GRANT CREATE SESSION TO " + TEST_USER);
            connection.execute("GRANT CREATE TABLE TO " + TEST_USER);
            connection.execute("GRANT SELECT ON V_$DATABASE TO " + TEST_USER);
            connection.execute("GRANT SELECT ON V_$TRANSACTION TO " + TEST_USER);
            connection.execute("GRANT SELECT ON V_$YSTREAM_SERVER TO " + TEST_USER);
            connection.execute("GRANT SELECT ANY TABLE TO " + TEST_USER);
            connection.execute("GRANT FLASHBACK ANY TABLE TO " + TEST_USER);
            connection.execute("GRANT YSTREAM_CAPTURE TO " + TEST_USER);
            connection.execute("GRANT ALTER SESSION TO " + TEST_USER);
        }
    }

    public static void createYStreamServer() throws Exception {
        try (YashanDbConnection connection = adminConnection()) {
            connection.connect();

            // Step 1: Configure YStream memory pool (use 128M to stay within shared pool limits)
            try {
                connection.execute("ALTER SYSTEM SET STREAM_POOL_SIZE = '128M'");
            }
            catch (Exception e) {
                // May already be set
            }

            // Step 2: Enable supplemental logging (database-level)
            try {
                connection.execute("ALTER DATABASE ADD SUPPLEMENTAL LOG TABLE TYPE (HEAP)");
            }
            catch (Exception e) {
                // May already be enabled
            }
            try {
                connection.execute("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            }
            catch (Exception e) {
                // May already be enabled
            }

            // Stop and drop existing YStream server if it exists, to ensure clean state
            String status = connection.queryAndMap(
                    "SELECT STATUS FROM SYS.V_$YSTREAM_SERVER WHERE SERVER_NAME = '" + DEFAULT_YSTREAM_SERVER + "'",
                    rs -> rs.next() ? rs.getString(1) : null);

            if (status != null) {
                // Stop existing server
                try {
                    connection.execute("BEGIN DBMS_YSTREAM_ADM.STOP('" + DEFAULT_YSTREAM_SERVER + "'); END;");
                }
                catch (Exception ignored) {
                    // May already be stopped
                }
                Thread.sleep(1000);
                // Drop existing server
                try {
                    connection.execute("BEGIN DBMS_YSTREAM_ADM.DROP('" + DEFAULT_YSTREAM_SERVER + "'); END;");
                }
                catch (Exception ignored) {
                    // Best-effort drop
                }
                Thread.sleep(1000);
            }

            // Step 3: Get current SCN for the start position
            Long currentScn = connection.queryAndMap(
                    "SELECT CURRENT_SCN FROM V$DATABASE",
                    rs -> rs.next() ? rs.getLong(1) : 0L);

            // Step 4: Create the YStream server
            connection.execute("BEGIN DBMS_YSTREAM_ADM.CREATE('" + DEFAULT_YSTREAM_SERVER + "', '" + TEST_USER + "', " + currentScn + "); END;");
            // Step 5: Start the YStream server
            connection.execute("BEGIN DBMS_YSTREAM_ADM.START('" + DEFAULT_YSTREAM_SERVER + "'); END;");

            // Wait for YStream server to become ready
            Thread.sleep(3000);
        }
    }

    /**
     * Add tables to the YStream service for change capture.
     * Must be called after the tables are created and before the connector starts streaming.
     * The YStream server is stopped before adding tables and restarted after.
     *
     * @param tableNames comma-separated table names (e.g. "TABLE_A,TABLE_B")
     */
    public static void addYStreamTables(String tableNames) throws Exception {
        // Build schema-qualified table names: DBZ.TABLE1,DBZ.TABLE2
        String[] names = tableNames.split(",");
        StringBuilder qualified = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            if (i > 0) {
                qualified.append(",");
            }
            qualified.append(TEST_SCHEMA).append(".").append(names[i].trim());
        }

        try (YashanDbConnection connection = adminConnection()) {
            connection.connect();

            // Check current YStream status to avoid errors from redundant stop/start
            String status = connection.queryAndMap(
                    "SELECT STATUS FROM SYS.V_$YSTREAM_SERVER WHERE SERVER_NAME = '" + DEFAULT_YSTREAM_SERVER + "'",
                    rs -> rs.next() ? rs.getString(1) : null);

            // Stop YStream server if running, before modifying table whitelist
            if ("RUNNING".equalsIgnoreCase(status) || "STARTED".equalsIgnoreCase(status)) {
                try {
                    connection.execute("BEGIN DBMS_YSTREAM_ADM.STOP('" + DEFAULT_YSTREAM_SERVER + "'); END;");
                }
                catch (Exception e) {
                    if (!containsMessage(e, "invalid")) {
                        throw e;
                    }
                }
                Thread.sleep(1000);
            }

            // Add tables: use schema-qualified names, schemas=null
            // Tolerate "already registered" errors since tables may persist across tests
            try {
                connection.execute("BEGIN DBMS_YSTREAM_ADM.ADD_TABLES('"
                        + DEFAULT_YSTREAM_SERVER + "', '"
                        + qualified + "', NULL); END;");
            }
            catch (Exception e) {
                if (!containsMessage(e, "already") && !containsMessage(e, "exist")) {
                    throw e;
                }
                // Tables already registered with YStream - this is OK
            }

            // Start YStream server
            connection.execute("BEGIN DBMS_YSTREAM_ADM.START('" + DEFAULT_YSTREAM_SERVER + "'); END;");
            Thread.sleep(2000);
        }
    }

    /**
     * Stop the default YStream server if it is running.
     * Releases any table locks held by the streaming engine so that
     * cleanup operations (DROP TABLE) can proceed.
     */
    public static void stopYStreamIfRunning() throws Exception {
        try (YashanDbConnection connection = adminConnection()) {
            connection.connect();
            String status = connection.queryAndMap(
                    "SELECT STATUS FROM SYS.V_$YSTREAM_SERVER WHERE SERVER_NAME = '" + DEFAULT_YSTREAM_SERVER + "'",
                    rs -> rs.next() ? rs.getString(1) : null);
            if (status != null) {
                try {
                    connection.execute("BEGIN DBMS_YSTREAM_ADM.STOP('" + DEFAULT_YSTREAM_SERVER + "'); END;");
                    Thread.sleep(1000);
                }
                catch (Exception ignored) {
                    // best-effort
                }
            }
        }
    }

    /**
     * Recreate the YStream server and register tables for capture.
     * Performs: drop existing server (if any) → create new server →
     * add tables → start server.  This gives each test a clean YStream
     * instance and avoids lock/stop-start issues.
     */
    public static void recreateYStreamServerWithTables(String tableNames) throws Exception {
        // Build schema-qualified table names
        String[] names = tableNames.split(",");
        StringBuilder qualified = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            if (i > 0) {
                qualified.append(",");
            }
            qualified.append(TEST_SCHEMA).append(".").append(names[i].trim());
        }

        try (YashanDbConnection connection = adminConnection()) {
            connection.connect();

            // Drop existing server if present
            String status = connection.queryAndMap(
                    "SELECT STATUS FROM SYS.V_$YSTREAM_SERVER WHERE SERVER_NAME = '" + DEFAULT_YSTREAM_SERVER + "'",
                    rs -> rs.next() ? rs.getString(1) : null);
            if (status != null) {
                try {
                    connection.execute("BEGIN DBMS_YSTREAM_ADM.STOP('" + DEFAULT_YSTREAM_SERVER + "'); END;");
                }
                catch (Exception ignored) {
                }
                Thread.sleep(1000);
                try {
                    connection.execute("BEGIN DBMS_YSTREAM_ADM.DROP('" + DEFAULT_YSTREAM_SERVER + "'); END;");
                }
                catch (Exception ignored) {
                }
                Thread.sleep(1000);
            }

            // Get current SCN for the start position
            Long currentScn = connection.queryAndMap(
                    "SELECT CURRENT_SCN FROM V$DATABASE",
                    rs -> rs.next() ? rs.getLong(1) : 0L);

            // Create fresh YStream server
            connection.execute("BEGIN DBMS_YSTREAM_ADM.CREATE('" + DEFAULT_YSTREAM_SERVER + "', '" + TEST_USER + "', " + currentScn + "); END;");

            // Add tables (YStream is in STOPPED state after CREATE, so ADD_TABLES works directly)
            connection.execute("BEGIN DBMS_YSTREAM_ADM.ADD_TABLES('"
                    + DEFAULT_YSTREAM_SERVER + "', '"
                    + qualified + "', NULL); END;");

            // Start YStream server
            connection.execute("BEGIN DBMS_YSTREAM_ADM.START('" + DEFAULT_YSTREAM_SERVER + "'); END;");
            Thread.sleep(3000);
        }
    }

    public static void dropTable(YashanDbConnection connection, String tableName) {
        try {
            connection.execute("DROP TABLE " + tableName);
        }
        catch (Exception e) {
            if (!isDoesNotExistError(e) && !isLockTimeoutError(e)) {
                throw new IllegalStateException("Could not drop table " + tableName, e);
            }
        }
    }

    public static void dropTables(YashanDbConnection connection, String... tableNames) {
        for (String table : tableNames) {
            dropTable(connection, table);
        }
    }

    public static String qualifiedTableName(String tableName) {
        return TestHelper.TEST_SCHEMA + "." + tableName;
    }

    private static String qualifiedDataCollectionId(String tableName) {
        return TestHelper.TEST_DATABASE + "." + TestHelper.qualifiedTableName(tableName);
    }

    /**
     * Create a table, ignoring "already exists" errors so that a previous
     * DROP TABLE that failed due to lock timeout does not block re-creation.
     */
    public static void createTableIgnoreExists(YashanDbConnection connection, String createSql) {
        try {
            connection.execute(createSql);
        }
        catch (Exception e) {
            if (!isAlreadyExistsError(e)) {
                throw new IllegalStateException("Could not create table", e);
            }
        }
    }

    /**
     * Drops all tables visible to user {@link #TEST_USER}.
     */
    public static void dropAllTables() {
        try (YashanDbConnection connection = testConnection()) {
            connection.query("SELECT TABLE_NAME FROM USER_TABLES", rs -> {
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    if (isQuoteRequired(tableName)) {
                        tableName = "\"" + tableName + "\"";
                    }
                    dropTable(connection, String.format("%s.%s", TEST_USER, tableName));
                }
            });
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to clean database", e);
        }
    }

    public static boolean isQuoteRequired(String tableName) {
        if (!Strings.isNullOrBlank(tableName)) {
            // Make sure table isn't already quoted
            if (!tableName.startsWith("\"") && !tableName.endsWith("\"")) {
                for (int i = 0; i < tableName.length(); i++) {
                    final char c = tableName.charAt(i);
                    // If we detect any lower case character or non letter/digit, name must be quoted
                    if (Character.isLowerCase(c) || !Character.isLetterOrDigit(c)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static String topicName(String tableName) {
        return TEST_DATABASE + "." + TEST_SCHEMA + "." + tableName;
    }

    public static String server() {
        return TestHelper.TEST_DATABASE;
    }

    private static boolean isAlreadyExistsError(Exception e) {
        return containsMessage(e, "already exists") || containsMessage(e, "already used")
                || containsErrorCode(e, "YAS-02013");
    }

    private static boolean isDoesNotExistError(Exception e) {
        return containsErrorCode(e, "YAS-02012") || containsErrorCode(e, "YAS-02012") || containsErrorCode(e, "YAS-02015")
                || containsMessage(e, "does not exist") || containsMessage(e, "not exist");
    }

    private static boolean isLockTimeoutError(Exception e) {
        return containsErrorCode(e, "YAS-02024") || containsMessage(e, "lock wait timeout");
    }

    private static boolean containsErrorCode(Exception e, String code) {
        return containsMessage(e, code);
    }

    private static boolean containsMessage(Throwable e, String value) {
        Throwable current = e;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().toLowerCase().contains(value.toLowerCase())) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static class LazyConnectionHolder {
        static final YashanDbConnection INSTANCE = new YashanDbConnection(TestHelper.testJdbcConfig().build());
    }

    public static YashanDbConnection testConnection() {
        return LazyConnectionHolder.INSTANCE;
    }
}
