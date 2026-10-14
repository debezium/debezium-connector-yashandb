/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.debezium.bean.DefaultBeanRegistry;
import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.yashandb.snapshot.lock.SharedSnapshotLock;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.snapshot.spi.SnapshotLock;

class YashanDbSnapshotLockTest {

    @FixFor("debezium/dbz#3014")
    @Test
    void selectsLockerByModeOutsideDialect() {
        var mainConnection = mock(YashanDbConnection.class);
        SnapshotLock snapshotLock = mock(SnapshotLock.class);
        assertThat(new YashanDbModeContext(false)
                .createSnapshotLocker(mainConnection, () -> mainConnection, snapshotLock))
                .extracting(locker -> locker.getClass().getSimpleName())
                .isEqualTo("YashanDbNativeSnapshotLocker");
        assertThat(new YashanDbModeContext(true)
                .createSnapshotLocker(mainConnection, () -> mainConnection, snapshotLock))
                .extracting(locker -> locker.getClass().getSimpleName())
                .isEqualTo("YashanDbMySqlSnapshotLocker");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void mysqlLocksAllTablesOnSeparateConnectionAndUnlocks() throws Exception {
        LockFixture fixture = new LockFixture(true);
        fixture.context.capturedTables = new LinkedHashSet<>(List.of(new TableId(null, "inventory", "orders"), new TableId(null, "other", "items")));

        fixture.source.lockTablesForSchemaSnapshot(fixture.running, fixture.context);
        fixture.source.releaseSchemaSnapshotLocks(fixture.context);

        String quote = String.valueOf((char) 96);
        verify(fixture.lockStatement).execute("LOCK TABLES " + quote + "inventory" + quote + "." + quote + "orders" + quote
                + " READ, " + quote + "other" + quote + "." + quote + "items" + quote + " READ");
        verify(fixture.lockStatement).execute("UNLOCK TABLES");
        verify(fixture.lockConnection).close();
        verify(fixture.mainJdbc, never()).createStatement();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void mysqlUnlocksWhenLockStatementFails() throws Exception {
        LockFixture fixture = new LockFixture(true);
        fixture.context.capturedTables = new LinkedHashSet<>(List.of(new TableId(null, "inventory", "orders")));
        String quote = String.valueOf((char) 96);
        when(fixture.lockStatement.execute("LOCK TABLES " + quote + "inventory" + quote + "." + quote + "orders" + quote + " READ"))
                .thenThrow(new SQLException("lock failed"));

        assertThatThrownBy(() -> fixture.source.lockTablesForSchemaSnapshot(fixture.running, fixture.context))
                .isInstanceOf(SQLException.class).hasMessage("lock failed");
        verify(fixture.lockStatement).execute("UNLOCK TABLES");
        verify(fixture.lockConnection).close();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void nativeLocksOnMainConnectionAndRollsBackToSavepoint() throws Exception {
        LockFixture fixture = new LockFixture(false);
        fixture.context.capturedTables = new LinkedHashSet<>(List.of(new TableId("", "APP", "ORDERS")));
        Savepoint savepoint = mock(Savepoint.class);
        when(fixture.mainJdbc.setSavepoint("dbz_schema_snapshot")).thenReturn(savepoint);
        when(fixture.mainConnection.quotedTableIdString(new TableId("", "APP", "ORDERS"))).thenReturn("\"APP\".\"ORDERS\"");

        fixture.source.lockTablesForSchemaSnapshot(fixture.running, fixture.context);
        fixture.source.releaseSchemaSnapshotLocks(fixture.context);

        verify(fixture.mainStatement).execute("LOCK TABLE \"APP\".\"ORDERS\" IN SHARE MODE");
        verify(fixture.mainJdbc).rollback(savepoint);
        verify(fixture.lockConnection, never()).connection();
    }

    private static class LockFixture {
        final YashanDbConnection mainConnection = mock(YashanDbConnection.class);
        final YashanDbConnection lockConnection = mock(YashanDbConnection.class);
        final Connection mainJdbc = mock(Connection.class);
        final Connection lockJdbc = mock(Connection.class);
        final Statement mainStatement = mock(Statement.class);
        final Statement lockStatement = mock(Statement.class);
        final ChangeEventSourceContext running = mock(ChangeEventSourceContext.class);
        final YashanDbSnapshotChangeEventSource source;
        final RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext> context;

        LockFixture(boolean mysqlMode) throws SQLException {
            YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
            when(config.getSignalingDataCollectionIds()).thenReturn(List.of());
            when(config.getSnapshotLockingMode()).thenReturn(Optional.of(YashanDbConnectorConfig.SnapshotLockingMode.SHARED));
            when(config.getModeContext()).thenReturn(new YashanDbModeContext(mysqlMode));
            BeanRegistry beanRegistry = new DefaultBeanRegistry();
            when(config.getBeanRegistry()).thenReturn(beanRegistry);
            MainConnectionProvidingConnectionFactory<YashanDbConnection> factory = mock(MainConnectionProvidingConnectionFactory.class);
            when(factory.mainConnection()).thenReturn(mainConnection);
            when(factory.newConnection()).thenReturn(lockConnection);
            when(mainConnection.connection()).thenReturn(mainJdbc);
            when(lockConnection.connection()).thenReturn(lockJdbc);
            when(mainJdbc.createStatement()).thenReturn(mainStatement);
            when(lockJdbc.createStatement()).thenReturn(lockStatement);
            when(running.isRunning()).thenReturn(true);
            SnapshotterService snapshotterService = mock(SnapshotterService.class);
            SharedSnapshotLock snapshotLock = new SharedSnapshotLock();
            snapshotLock.injectBeanRegistry(beanRegistry);
            when(snapshotterService.getSnapshotLock()).thenReturn(snapshotLock);
            source = new YashanDbSnapshotChangeEventSource(config, factory, mock(YashanDbDatabaseSchema.class), null, null, null, null, snapshotterService);
            context = (RelationalSnapshotContext<YashanDbPartition, YashanDbOffsetContext>) source.prepare(new YashanDbPartition("server", "database"), false);
            beanRegistry.add(StandardBeanNames.CONNECTOR_CONFIG, config);
            beanRegistry.add(StandardBeanNames.SNAPSHOT_CONTEXT, context);
        }
    }
}
