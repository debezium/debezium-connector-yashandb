/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.metadata;

import java.util.List;

import io.debezium.connector.yashandb.Module;
import io.debezium.connector.yashandb.YashanDbConnector;
import io.debezium.connector.yashandb.converters.DateToStringConverter;
import io.debezium.connector.yashandb.converters.TimeToStringConverter;
import io.debezium.connector.yashandb.converters.TimestampToStringConverter;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all YashanDB connector and custom converter metadata.
 */
public class YashanDbMetadataProvider implements ComponentMetadataProvider {

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    /**
     * Returns the component metadata for the YashanDB connector and its custom converters.
     *
     * @return the list of component metadata entries
     */
    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                componentMetadataFactory.createComponentMetadata(new YashanDbConnector(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new DateToStringConverter(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new TimeToStringConverter(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new TimestampToStringConverter(), Module.version()));
    }
}