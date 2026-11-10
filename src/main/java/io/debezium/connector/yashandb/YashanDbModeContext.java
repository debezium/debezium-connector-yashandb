/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.util.Map;
import java.util.function.Supplier;

import io.debezium.connector.yashandb.dialect.YashanDbDialect;
import io.debezium.connector.yashandb.dialect.YashanDbMetadataReader;
import io.debezium.connector.yashandb.dialect.YashanDbMySqlDialect;
import io.debezium.connector.yashandb.dialect.YashanDbMySqlMetadataReader;
import io.debezium.connector.yashandb.dialect.YashanDbNativeDialect;
import io.debezium.connector.yashandb.dialect.YashanDbNativeMetadataReader;
import io.debezium.connector.yashandb.dialect.YashanDbSnapshotLocker;
import io.debezium.connector.yashandb.dialect.YashanDbSnapshotLockerFactory;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.snapshot.spi.SnapshotLock;

/** Immutable mode selection shared by JDBC, schema, snapshots and streaming. */
public final class YashanDbModeContext {

    public static final String OFFSET_KEY = "database_mode";

    private final YashanDbDialect dialect;
    private final YashanDbMetadataReader metadataReader;

    public YashanDbModeContext(boolean mysqlMode) {
        dialect = mysqlMode ? new YashanDbMySqlDialect() : new YashanDbNativeDialect();
        metadataReader = mysqlMode ? new YashanDbMySqlMetadataReader() : new YashanDbNativeMetadataReader();
    }

    public boolean isMySql() {
        return dialect instanceof YashanDbMySqlDialect;
    }

    public String name() {
        return dialect.name();
    }

    public YashanDbDialect dialect() {
        return dialect;
    }

    public YashanDbMetadataReader metadataReader() {
        return metadataReader;
    }

    public YashanDbSnapshotLocker createSnapshotLocker(JdbcConnection mainConnection,
                                                       Supplier<? extends JdbcConnection> lockConnectionFactory,
                                                       SnapshotLock snapshotLock) {
        return YashanDbSnapshotLockerFactory.create(isMySql(), mainConnection, lockConnectionFactory, snapshotLock);
    }

    public void validateOffset(Map<String, ?> offset) {
        Object stored = offset.get(OFFSET_KEY);
        if (stored == null && !isMySql()) {
            return;
        }
        if (!name().equals(stored)) {
            throw new IllegalArgumentException("Stored database_mode '" + stored + "' does not match connector mode '" + name() + "'");
        }
    }
}
