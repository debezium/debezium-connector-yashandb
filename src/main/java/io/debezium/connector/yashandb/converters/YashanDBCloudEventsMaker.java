/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.converters;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

public class YashanDBCloudEventsMaker extends CloudEventsMaker {

    public YashanDBCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        super(parser, contentType, dataSchemaUriBase);
    }

    @Override
    public String ceId() {
        return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";scn:" + recordParser.getMetadata(YashanDBRecordParser.SCN_KEY)
                + ";commit_scn:" + recordParser.getMetadata(YashanDBRecordParser.COMMIT_SCN_KEY)
                + ";lcr_position:" + recordParser.getMetadata(YashanDBRecordParser.LCR_POSITION_KEY);
    }
}
