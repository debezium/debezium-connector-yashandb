/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import io.debezium.schema.SchemaFactory;

public class YashanDbSchemaFactory extends SchemaFactory {

    public YashanDbSchemaFactory() {
        super();
    }

    private static final YashanDbSchemaFactory yashanDbSchemaFactoryObject = new YashanDbSchemaFactory();

    public static YashanDbSchemaFactory get() {
        return yashanDbSchemaFactoryObject;
    }

}
