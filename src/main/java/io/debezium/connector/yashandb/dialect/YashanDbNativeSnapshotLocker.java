/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Collection;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;
import io.debezium.snapshot.spi.SnapshotLock;

/**
 * Locks native-mode tables on the snapshot connection and releases them by rolling back to the
 * savepoint created before the first lock.
 */
final class YashanDbNativeSnapshotLocker implements YashanDbSnapshotLocker {

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDbNativeSnapshotLocker.class);
    private static final String SAVEPOINT_NAME = "dbz_schema_snapshot";

    private final JdbcConnection connection;
    private final SnapshotLock snapshotLock;
    private Savepoint savepoint;
    private boolean locked;

    YashanDbNativeSnapshotLocker(JdbcConnection connection, SnapshotLock snapshotLock) {
        this.connection = connection;
        this.snapshotLock = snapshotLock;
    }

    @Override
    public void lock(ChangeEventSourceContext sourceContext, Collection<TableId> tables) throws SQLException, InterruptedException {
        savepoint = connection.connection().setSavepoint(SAVEPOINT_NAME);
        locked = true;
        try (Statement statement = connection.connection().createStatement()) {
            for (TableId tableId : tables) {
                if (!sourceContext.isRunning()) {
                    throw new InterruptedException("Interrupted while locking table " + tableId);
                }

                Optional<String> lockingStatement = snapshotLock.tableLockingStatement(null, connection.quotedTableIdString(tableId));
                if (lockingStatement.isPresent()) {
                    LOGGER.debug("Locking table {}", tableId);
                    statement.execute(lockingStatement.get());
                }
            }
        }
        catch (SQLException | InterruptedException | RuntimeException e) {
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
        if (locked) {
            locked = false;
            connection.connection().rollback(savepoint);
        }
    }
}
