/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.snapshot.lock;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.connector.yashandb.YashanDbConnector;
import io.debezium.connector.yashandb.YashanDbConnectorConfig;
import io.debezium.connector.yashandb.dialect.YashanDbDialect;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;
import io.debezium.snapshot.spi.SnapshotLock;

@ConnectorSpecific(connector = YashanDbConnector.class)
public class SharedSnapshotLock implements SnapshotLock, BeanRegistryAware {

    private BeanRegistry beanRegistry;

    @Override
    public String name() {
        return YashanDbConnectorConfig.SnapshotLockingMode.SHARED.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public void injectBeanRegistry(BeanRegistry beanRegistry) {
        this.beanRegistry = beanRegistry;
    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
        YashanDbConnectorConfig connectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, YashanDbConnectorConfig.class);
        RelationalSnapshotContext<?, ?> snapshotContext = beanRegistry.lookupByName(StandardBeanNames.SNAPSHOT_CONTEXT, RelationalSnapshotContext.class);
        Collection<TableId> capturedTables = snapshotContext == null ? List.of() : snapshotContext.capturedTables;
        YashanDbDialect dialect = connectorConfig.getModeContext().dialect();
        return Optional.of(dialect.lockTablesStatement(tableId, capturedTables));
    }
}
