/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Module}.
 */
class ModuleTest {

    @Test
    void shouldReturnModuleName() {
        assertThat(Module.name()).isEqualTo("yashandb");
    }

    @Test
    void shouldReturnModuleContextName() {
        assertThat(Module.contextName()).isEqualTo("YashanDB");
    }

    @Test
    void shouldReturnModuleVersion() {
        String version = Module.version();
        // Version is loaded from build.version resource file
        // May be null/${project.version} if not built with Maven
        assertThat(version).isNotNull();
    }
}
