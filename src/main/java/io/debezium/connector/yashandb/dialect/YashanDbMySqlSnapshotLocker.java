/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;
import io.debezium.snapshot.spi.SnapshotLock;

/**
 * Locks MySQL-mode tables on a dedicated connection. MySQL keeps table locks per session, so the
 * snapshot connection remains free to read the schema and all locks are released together.
 */
final class YashanDbMySqlSnapshotLocker implements YashanDbSnapshotLocker {

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDbMySqlSnapshotLocker.class);

    private final Supplier<? extends JdbcConnection> lockConnectionFactory;
    private final SnapshotLock snapshotLock;
    private JdbcConnection lockConnection;

    YashanDbMySqlSnapshotLocker(Supplier<? extends JdbcConnection> lockConnectionFactory, SnapshotLock snapshotLock) {
        this.lockConnectionFactory = lockConnectionFactory;
        this.snapshotLock = snapshotLock;
    }

    @Override
    public void lock(ChangeEventSourceContext sourceContext, Collection<TableId> tables) throws SQLException, InterruptedException {
        for (TableId tableId : tables) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while locking table " + tableId);
            }
        }
        if (tables.isEmpty()) {
            return;
        }

        lockConnection = lockConnectionFactory.get();
        try {
            try (Statement statement = lockConnection.connection().createStatement()) {
                Optional<String> lockingStatement = snapshotLock.tableLockingStatement(null,
                        lockConnection.quotedTableIdString(tables.iterator().next()));
                if (lockingStatement.isPresent()) {
                    LOGGER.debug("Locking tables {}", tables);
                    statement.execute(lockingStatement.get());
                }
            }
        }
        catch (SQLException e) {
            try {
                unlock();
            }
            catch (SQLException unlockFailure) {
                e.addSuppressed(unlockFailure);
            }
            throw e;
        }
    }

    @Override
    public void unlock() throws SQLException {
        JdbcConnection connection = lockConnection;
        if (connection != null) {
            lockConnection = null;
            try (JdbcConnection closingConnection = connection;
                    Statement statement = closingConnection.connection().createStatement()) {
                statement.execute("UNLOCK TABLES");
            }
        }
    }
}
