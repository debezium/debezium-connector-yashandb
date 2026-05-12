/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yashandb.ystream;

/**
 * This interface allows the YashanDB schema change event emitter to pass a truncate event
 * back to the caller to be handled differently.
 */
public interface TruncateReceiver {
    /**
     * Notify the receiver of the truncate event
     */
    void processTruncateEvent();
}
