/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

public class YashanDBPartition extends AbstractPartition implements Partition {
    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    public YashanDBPartition(String serverName, String databaseName) {
        super(databaseName);
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final YashanDBPartition other = (YashanDBPartition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return serverName.hashCode();
    }

    @Override
    public String toString() {
        return "YashanDBPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<YashanDBPartition> {
        private final YashanDBConnectorConfig connectorConfig;

        Provider(YashanDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<YashanDBPartition> getPartitions() {

            return Collections.singleton(new YashanDBPartition(connectorConfig.getLogicalName(), connectorConfig.getDatabaseName()));
        }
    }
}
