/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import io.debezium.schema.SchemaFactory;

public class YashanDBSchemaFactory extends SchemaFactory {

    public YashanDBSchemaFactory() {
        super();
    }

    private static final YashanDBSchemaFactory oracleSchemaFactoryObject = new YashanDBSchemaFactory();

    public static YashanDBSchemaFactory get() {
        return oracleSchemaFactoryObject;
    }

}
