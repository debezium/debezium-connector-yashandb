/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.util.function.Supplier;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.snapshot.spi.SnapshotLock;

/** Selects the snapshot lock implementation for the configured database mode. */
public final class YashanDbSnapshotLockerFactory {

    private YashanDbSnapshotLockerFactory() {
    }

    public static YashanDbSnapshotLocker create(boolean mysqlMode, JdbcConnection mainConnection,
                                                Supplier<? extends JdbcConnection> lockConnectionFactory, SnapshotLock snapshotLock) {
        return mysqlMode ? new YashanDbMySqlSnapshotLocker(lockConnectionFactory, snapshotLock)
                : new YashanDbNativeSnapshotLocker(mainConnection, snapshotLock);
    }
}
