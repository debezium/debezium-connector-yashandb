/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.util.Properties;

import io.debezium.util.IoUtil;

/**
 * Provides metadata information about the YashanDB connector module, including version and plugin name.
 *
 * @author Debezium Authors
 */
public final class Module {

    private static final Properties INFO = IoUtil.loadProperties(Module.class, "io/debezium/connector/yashandb/build.version");

    /**
     * Returns the version of this connector module.
     *
     * @return the module version string
     */
    public static String version() {
        return INFO.getProperty("version");
    }

    /**
     * @return symbolic name of the connector plugin
     */
    public static String name() {
        return "yashandb";
    }

    /**
     * @return context name used in log MDC and JMX metrics
     */
    public static String contextName() {
        return "YashanDB";
    }
}
