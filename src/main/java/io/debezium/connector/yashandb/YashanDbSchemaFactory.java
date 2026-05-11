/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import io.debezium.schema.SchemaFactory;

/**
 * Schema factory for YashanDB.
 */
public class YashanDbSchemaFactory extends SchemaFactory {

    /**
     * Creates a new schema factory instance.
     */
    public YashanDbSchemaFactory() {
        super();
    }

    private static final YashanDbSchemaFactory yashanDbSchemaFactoryObject = new YashanDbSchemaFactory();

    /**
     * Returns the singleton YashanDB schema factory instance.
     *
     * @return the singleton schema factory
     */
    public static YashanDbSchemaFactory get() {
        return yashanDbSchemaFactoryObject;
    }

}
