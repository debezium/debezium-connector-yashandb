/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

/**
 * Creates source info structs for YashanDB change events.
 */
public class YashanDbSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        this.schema = commonSchemaBuilder()
                .name("io.debezium.connector.yashandb.Source")
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TXID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.BATCH_ROW_ID_KEY, Schema.OPTIONAL_INT32_SCHEMA)
                .field(SourceInfo.POSITION_SCN_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.GROUP_LSN_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.GROUP_OFFSET_KEY, Schema.OPTIONAL_INT32_SCHEMA)
                .field(SourceInfo.INSTANCE_ID_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.USERNAME_KEY, Schema.OPTIONAL_STRING_SCHEMA).build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct ret = super.commonStruct(sourceInfo)
                .put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.tableSchema())
                .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.table())
                .put(SourceInfo.TXID_KEY, sourceInfo.getTransactionId());

        if (sourceInfo.getLcrPosition() != null) {
            ret.put(SourceInfo.POSITION_SCN_KEY, sourceInfo.getPositionScn());
            ret.put(SourceInfo.GROUP_LSN_KEY, sourceInfo.getGroupLsn());
            ret.put(SourceInfo.GROUP_OFFSET_KEY, sourceInfo.getGroupOffset());
            ret.put(SourceInfo.INSTANCE_ID_KEY, sourceInfo.getInstanceId());
            ret.put(SourceInfo.BATCH_ROW_ID_KEY, sourceInfo.getBatchRowId());
        }

        return ret;
    }
}
