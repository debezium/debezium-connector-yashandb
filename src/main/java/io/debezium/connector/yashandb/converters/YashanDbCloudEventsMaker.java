/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.converters;

import java.util.Set;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.SerializerType;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * CloudEvents maker implementation for YashanDB.
 */
public class YashanDbCloudEventsMaker extends CloudEventsMaker {

    public static final String SCN_KEY = "scn";
    public static final String COMMIT_SCN_KEY = "commit_scn";
    public static final String LCR_POSITION_KEY = "lcr_position";

    static final Set<String> YASHANDB_SOURCE_FIELDS = Collect.unmodifiableSet(
            SCN_KEY,
            COMMIT_SCN_KEY,
            LCR_POSITION_KEY);

    /**
     * Creates a new CloudEvents maker for YashanDB with the given record and schema context.
     *
     * @param recordAndMetadata the record and associated metadata
     * @param dataContentType the serialization format for the CloudEvent data
     * @param dataSchemaUriBase the base URI for data schema resolution
     * @param cloudEventsSchemaName the CloudEvents schema name
     */
    public YashanDbCloudEventsMaker(RecordAndMetadata recordAndMetadata, SerializerType dataContentType, String dataSchemaUriBase,
                                    String cloudEventsSchemaName) {
        super(recordAndMetadata, dataContentType, dataSchemaUriBase, cloudEventsSchemaName, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    /**
     * Generates a unique CloudEvents identifier based on YashanDB source metadata fields.
     *
     * @return the CloudEvents id string
     */
    @Override
    public String ceId() {
        return "name:" + sourceField(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";scn:" + sourceField(SCN_KEY)
                + ";commit_scn:" + sourceField(COMMIT_SCN_KEY)
                + ";lcr_position:" + sourceField(LCR_POSITION_KEY);
    }

    /**
     * Returns the set of YashanDB-specific source field names to include in CloudEvents.
     *
     * @return the set of connector-specific source field names
     */
    @Override
    public Set<String> connectorSpecificSourceFields() {
        return YASHANDB_SOURCE_FIELDS;
    }
}