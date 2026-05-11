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

/**
 * Represents the source partition for the YashanDB connector, identified by the server name.
 * <p>
 * This partition is used to track the position of change events within a specific server instance.
 *
 * @author Debezium Authors
 */
public class YashanDbPartition extends AbstractPartition implements Partition {
    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    /**
     * Creates a YashanDbPartition instance for the given server and database.
     *
     * @param serverName the logical server name
     * @param databaseName the database name
     */
    public YashanDbPartition(String serverName, String databaseName) {
        super(databaseName);
        this.serverName = serverName;
    }

    /**
     * Returns the source partition map containing the server name.
     *
     * @return the partition map
     */
    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    /**
     * Compares this partition to another object for equality based on server name.
     *
     * @param obj the object to compare to
     * @return true if the objects have the same server name, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final YashanDbPartition other = (YashanDbPartition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    /**
     * Returns a hash code for this partition based on the server name.
     *
     * @return the hash code value
     */
    @Override
    public int hashCode() {
        return serverName.hashCode();
    }

    /**
     * Returns a string representation of this partition.
     *
     * @return the string representation
     */
    @Override
    public String toString() {
        return "YashanDbPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    /**
     * Provides partition instances for the YashanDB connector based on the connector configuration.
     */
    static class Provider implements Partition.Provider<YashanDbPartition> {
        private final YashanDbConnectorConfig connectorConfig;

        /**
         * Creates a Provider instance with the given connector configuration.
         *
         * @param connectorConfig the connector configuration
         */
        Provider(YashanDbConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        /**
         * Returns the set of partitions for this connector, consisting of a single partition
         * identified by the logical server name and database name.
         *
         * @return the singleton set of YashanDbPartition
         */
        @Override
        public Set<YashanDbPartition> getPartitions() {

            return Collections.singleton(new YashanDbPartition(connectorConfig.getLogicalName(), connectorConfig.getDatabaseName()));
        }
    }
}
