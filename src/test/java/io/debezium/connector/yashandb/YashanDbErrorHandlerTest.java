/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.sql.SQLRecoverableException;

import org.junit.jupiter.api.Test;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Unit tests for {@link YashanDbErrorHandler}.
 */
class YashanDbErrorHandlerTest {

    @Test
    void shouldNotBeRetriableForGenericException() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        ChangeEventQueue<?> queue = mock(ChangeEventQueue.class);
        ErrorHandler replaced = mock(ErrorHandler.class);
        YashanDbErrorHandler handler = new YashanDbErrorHandler(config, queue, replaced);
        assertThat(handler.isRetriable(new RuntimeException("generic"))).isFalse();
    }

    @Test
    void shouldBeRetriableForSQLRecoverableException() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        ChangeEventQueue<?> queue = mock(ChangeEventQueue.class);
        ErrorHandler replaced = mock(ErrorHandler.class);
        YashanDbErrorHandler handler = new YashanDbErrorHandler(config, queue, replaced);
        SQLRecoverableException ex = mock(SQLRecoverableException.class);
        assertThat(handler.isRetriable(ex)).isTrue();
    }

    @Test
    void shouldNotBeRetriableForIOExceptionAtTopLevel() {
        // IOException is only retriable when nested as a cause, not at top level
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        ChangeEventQueue<?> queue = mock(ChangeEventQueue.class);
        ErrorHandler replaced = mock(ErrorHandler.class);
        YashanDbErrorHandler handler = new YashanDbErrorHandler(config, queue, replaced);
        // At top level without a cause chain that goes through getCause()
        IOException ex = new IOException("connection reset");
        // Actually, IOException check happens in getCause() path
        // When the top-level exception is IOException itself, it goes through the while loop
        // The first check is SQLRecoverableException (no), then message check (no codes match),
        // then getCause() (null), then loop exits. So top-level IOException is not retriable.
        assertThat(handler.isRetriable(ex)).isFalse();
    }

    @Test
    void shouldBeRetriableForIOExceptionAsCause() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        ChangeEventQueue<?> queue = mock(ChangeEventQueue.class);
        ErrorHandler replaced = mock(ErrorHandler.class);
        YashanDbErrorHandler handler = new YashanDbErrorHandler(config, queue, replaced);
        RuntimeException top = new RuntimeException("wrapper", new IOException("connection reset"));
        assertThat(handler.isRetriable(top)).isTrue();
    }

    @Test
    void shouldNotBeRetriableForExceptionWithNullMessage() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        ChangeEventQueue<?> queue = mock(ChangeEventQueue.class);
        ErrorHandler replaced = mock(ErrorHandler.class);
        YashanDbErrorHandler handler = new YashanDbErrorHandler(config, queue, replaced);
        assertThat(handler.isRetriable(new RuntimeException())).isFalse();
    }

    @Test
    void shouldNotBeRetriableForNestedNonIOException() {
        YashanDbConnectorConfig config = mock(YashanDbConnectorConfig.class);
        ChangeEventQueue<?> queue = mock(ChangeEventQueue.class);
        ErrorHandler replaced = mock(ErrorHandler.class);
        YashanDbErrorHandler handler = new YashanDbErrorHandler(config, queue, replaced);
        RuntimeException top = new RuntimeException("wrapper", new IllegalArgumentException("bad arg"));
        assertThat(handler.isRetriable(top)).isFalse();
    }
}
