/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.sics.ystream.conf.StartMode;
import com.sics.ystream.conf.YstreamConfig;
import com.sics.ystream.result.Position;

import io.debezium.doc.FixFor;

/**
 * Guards the reflective mapping of {@code ystream.additional.properties.*} options to YStream builder setters.
 */
class YStreamConfigBuilderTest {

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldApplyBooleanOptionToBuilder() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        YStreamConfigBuilder.apply(builder, Collections.singletonMap("fault.tolerance", "true"));
        assertThat(builder.build().getFaultTolerance()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldApplyNumericOptionToBuilder() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        YStreamConfigBuilder.apply(builder, Collections.singletonMap("queue.size", "42"));
        assertThat(builder.build().getQueueSize()).isEqualTo(42);
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldApplyStringOptionToBuilder() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        YStreamConfigBuilder.apply(builder, Collections.singletonMap("host", "ystream.example"));
        assertThat(builder.build().getHost()).isEqualTo("ystream.example");
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldApplyEnumOptionToBuilderCaseInsensitively() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        builder.setRecoverPosition(new Position());
        YStreamConfigBuilder.apply(builder, Collections.singletonMap("start.mode", "recover"));
        assertThat(builder.build().getStartMode()).isEqualTo(StartMode.RECOVER);
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldApplyCharacterOptionToSetter() {
        CharacterBuilder builder = new CharacterBuilder();
        YStreamConfigBuilder.applyOptions(builder, Collections.singletonMap("delimiter", "|"));
        assertThat(builder.getDelimiter()).isEqualTo('|');
    }

    @ParameterizedTest
    @ValueSource(strings = { "1688", "sensitive-port-value" })
    @FixFor("debezium/dbz#2861")
    void shouldRejectOverloadedSetterBeforeConvertingOrApplyingValue(String value) {
        OverloadedBuilder builder = new OverloadedBuilder();
        assertThatThrownBy(() -> YStreamConfigBuilder.applyOptions(builder, Collections.singletonMap("port", value)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Ambiguous YStream builder option 'port': multiple matching setters")
                .hasMessageNotContaining(value)
                .hasNoCause();
        assertThat(builder.port).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldRejectSettersWithTheSameNormalizedName() {
        assertThatThrownBy(() -> YStreamConfigBuilder.applyOptions(new CollidingNameBuilder(), Collections.singletonMap("server.name", "server")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Ambiguous YStream builder option 'server.name': multiple matching setters")
                .hasNoCause();
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldApplyOverriddenSetterWithCovariantReturnType() {
        InheritedCharacterBuilder builder = new InheritedCharacterBuilder();
        YStreamConfigBuilder.applyOptions(builder, Collections.singletonMap("delimiter", "|"));
        assertThat(builder.getDelimiter()).isEqualTo('|');
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldApplyMysqlModeFromAdditionalProperties() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        YStreamConfigBuilder.apply(builder, Collections.singletonMap("mysql.mode", "true"));
        assertThat(builder.build().isMysqlMode()).isTrue();

        YstreamConfig.Builder<String> aliasBuilder = minimalBuilder();
        YStreamConfigBuilder.apply(aliasBuilder, Collections.singletonMap("MYSQL_MODE", "true"));
        assertThat(aliasBuilder.build().isMysqlMode()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldRejectInvalidMysqlModeAndAliasConflict() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, Collections.singletonMap("mysql.mode", "maybe")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected true or false");
        Map<String, String> aliases = new LinkedHashMap<>();
        aliases.put("mysql.mode", "true");
        aliases.put("mysql_mode", "false");
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, aliases))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate YStream additional property");
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldMapDottedNameToCamelCaseSetter() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        YStreamConfigBuilder.apply(builder, Collections.singletonMap("enable.sequence.next.val", "true"));
        assertThat(builder.build().isEnableSequenceNextVal()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldRejectEmptyOptionName() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, Collections.singletonMap("", "true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name must not be empty");
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldRejectUnknownOption() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, Collections.singletonMap("no.such.option", "true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported YStream builder option 'no.such.option'");
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldRejectDuplicateOption() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        Map<String, String> aliases = new LinkedHashMap<>();
        aliases.put("fault.tolerance", "true");
        aliases.put("fault_tolerance", "false");
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, aliases))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate YStream additional property: fault_tolerance");
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldRejectInvalidBooleanValue() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, Collections.singletonMap("fault.tolerance", "maybe")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fault.tolerance")
                .hasMessageContaining("Expected true or false")
                .hasMessageNotContaining("maybe")
                .hasNoCause();
    }

    @Test
    @FixFor("debezium/dbz#2861")
    void shouldReportInvalidNumericOptionWithoutExposingItsValue() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        String sensitiveValue = "sensitive-queue-size";
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, Collections.singletonMap("queue.size", sensitiveValue)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queue.size")
                .hasMessageContaining("int")
                .hasMessageNotContaining(sensitiveValue)
                .hasNoCause();
    }

    @ParameterizedTest
    @ValueSource(strings = { "password", "access.token", "API_KEY", "user-name" })
    @FixFor("debezium/dbz#2861")
    void shouldReportSensitiveOptionNameWithoutExposingItsValue(String name) {
        String sensitiveValue = "sensitive-password";

        assertThatThrownBy(() -> YStreamConfigBuilder.apply(minimalBuilder(), Collections.singletonMap(name, sensitiveValue)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Sensitive YStream builder option '" + name + "' cannot be configured through additional properties")
                .hasMessageNotContaining(sensitiveValue)
                .hasNoCause();
    }

    @Test
    @FixFor("debezium/dbz#2861")
    @SuppressWarnings("unchecked")
    void shouldReportSetterFailureWithoutExposingItsValueOrCause() {
        YstreamConfig.Builder<String> builder = mock(YstreamConfig.Builder.class);
        String sensitiveValue = "sensitive-value";
        when(builder.setHost(sensitiveValue)).thenThrow(new IllegalArgumentException("Rejected " + sensitiveValue));

        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, Collections.singletonMap("host", sensitiveValue)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unable to apply YStream builder option 'host'")
                .hasMessageNotContaining(sensitiveValue)
                .hasNoCause();
    }

    /**
     * A builder with all values that {@link YstreamConfig} validates at build time, so tests can assert on the built config.
     */
    private static YstreamConfig.Builder<String> minimalBuilder() {
        return YstreamConfig.<String> builder()
                .setHost("localhost")
                .setPort("1688")
                .setUser("user")
                .setPassword("password")
                .setServerName("server")
                .setDeserializer((lcr, metadata) -> null);
    }

    private static class CharacterBuilder {

        private char delimiter;

        public CharacterBuilder setDelimiter(char delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public char getDelimiter() {
            return delimiter;
        }
    }

    private static class OverloadedBuilder {

        private String port;

        public OverloadedBuilder setPort(String port) {
            this.port = port;
            return this;
        }

        public OverloadedBuilder setPort(int port) {
            this.port = Integer.toString(port);
            return this;
        }
    }

    private static class CollidingNameBuilder {

        public CollidingNameBuilder setServerName(String serverName) {
            return this;
        }

        public CollidingNameBuilder setServername(String serverName) {
            return this;
        }
    }

    private static class InheritedCharacterBuilder extends CharacterBuilder {

        @Override
        public InheritedCharacterBuilder setDelimiter(char delimiter) {
            super.setDelimiter(delimiter);
            return this;
        }
    }
}
