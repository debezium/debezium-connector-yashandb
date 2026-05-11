/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.converters;

import io.debezium.connector.yashandb.Module;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for YashanDB.
 */
public class YashanDbCloudEventsProvider implements CloudEventsProvider {
    /**
     * Returns the name of this CloudEvents provider.
     *
     * @return the provider name
     */
    @Override
    public String getName() {
        return Module.name();
    }

    /**
     * Creates a new CloudEvents maker for the given record and schema context.
     *
     * @param recordAndMetadata the record and associated metadata
     * @param dataContentType the serialization format for the CloudEvent data
     * @param dataSchemaUriBase the base URI for data schema resolution
     * @param cloudEventsSchemaName the CloudEvents schema name
     * @return a new YashanDbCloudEventsMaker instance
     */
    @Override
    public CloudEventsMaker createMaker(RecordAndMetadata recordAndMetadata, SerializerType dataContentType, String dataSchemaUriBase,
                                        String cloudEventsSchemaName) {
        return new YashanDbCloudEventsMaker(recordAndMetadata, dataContentType, dataSchemaUriBase, cloudEventsSchemaName);
    }
}