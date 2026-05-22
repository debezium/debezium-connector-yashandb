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
        // Given & When: get module version
        String version = Module.version();

        // Then: verify version format follows SemVer or Maven version spec
        // version should not contain placeholders (e.g., ${project.version})
        assertThat(version).isNotNull();
        assertThat(version).isNotEmpty()
                .describedAs("Version should not be empty");
        assertThat(version).matches("^\\d+\\.\\d+.*")
                .describedAs("Version should follow SemVer format (e.g., 1.0.0)");
        assertThat(version).doesNotContain("${")
                .describedAs("Version should not contain Maven placeholder");
    }
}
