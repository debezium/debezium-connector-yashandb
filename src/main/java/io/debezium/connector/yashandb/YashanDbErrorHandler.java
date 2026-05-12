/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.io.IOException;
import java.sql.SQLRecoverableException;
import java.util.Set;

import io.debezium.annotation.Immutable;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

/**
 * Error handle for YashanDB.
 */
public class YashanDbErrorHandler extends ErrorHandler {

    /**
     * Contents of this set should only be YAS-xxxxx errors;
     * The error check uses starts-with semantics
     */
    @Immutable
    private static final Set<String> RETRIABLE_ERROR_CODES = Collect.unmodifiableSet();

    /**
     * Contents of this set should be any type of error message text;
     * The error check uses case-insensitive contains semantics
     */
    @Immutable
    private static final Set<String> RETRIABLE_ERROR_MESSAGES = Collect.unmodifiableSet();

    public YashanDbErrorHandler(YashanDbConnectorConfig connectorConfig, ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        super(YashanDbConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        while (throwable != null) {
            // Always retry any recoverable error
            if (throwable instanceof SQLRecoverableException) {
                return true;
            }

            // If message is provided, run checks against it
            final String message = throwable.getMessage();
            if (message != null && message.length() > 0) {
                // Check YashanDB error codes
                for (String errorCode : RETRIABLE_ERROR_CODES) {
                    if (message.startsWith(errorCode)) {
                        return true;
                    }
                }
                // Check YashanDB error message texts
                for (String messageText : RETRIABLE_ERROR_MESSAGES) {
                    if (message.toUpperCase().contains(messageText.toUpperCase())) {
                        return true;
                    }
                }
            }

            if (throwable.getCause() != null) {
                // We explicitly check this below the top-level error as we only want
                // certain nested exceptions to be retried, not if they're at the top
                final Throwable cause = throwable.getCause();
                if (cause instanceof IOException) {
                    return true;
                }
            }

            throwable = throwable.getCause();
        }
        return false;
    }
}
