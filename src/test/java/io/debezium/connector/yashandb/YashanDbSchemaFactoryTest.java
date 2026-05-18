/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link YashanDbSchemaFactory}.
 */
class YashanDbSchemaFactoryTest {

    @Test
    void shouldReturnSingletonInstance() {
        YashanDbSchemaFactory factory1 = YashanDbSchemaFactory.get();
        YashanDbSchemaFactory factory2 = YashanDbSchemaFactory.get();
        assertThat(factory1).isSameAs(factory2);
    }

    @Test
    void shouldCreateNewInstance() {
        YashanDbSchemaFactory factory = new YashanDbSchemaFactory();
        assertThat(factory).isNotNull();
    }

    @Test
    void shouldBeInstanceOfSchemaFactory() {
        YashanDbSchemaFactory factory = YashanDbSchemaFactory.get();
        assertThat(factory).isInstanceOf(io.debezium.schema.SchemaFactory.class);
    }
}
