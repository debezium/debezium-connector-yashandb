/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.sql.SQLException;
import java.util.Collection;

import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * Acquires and releases the locks that protect table metadata while a schema snapshot is taken.
 * The mode context selects the strategy that is appropriate for its locking semantics.
 */
public interface YashanDbSnapshotLocker extends AutoCloseable {

    /**
     * Acquires locks for the supplied tables.
     *
     * @param sourceContext the snapshot source context used to observe cancellation
     * @param tables the captured snapshot tables
     */
    void lock(ChangeEventSourceContext sourceContext, Collection<TableId> tables) throws SQLException, InterruptedException;

    /**
     * Releases every lock acquired by this locker. Calling this method more than once has no effect.
     */
    void unlock() throws SQLException;

    @Override
    default void close() throws SQLException {
        unlock();
    }
}
