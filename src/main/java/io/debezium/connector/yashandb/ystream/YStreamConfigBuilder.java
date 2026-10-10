/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.sics.ystream.conf.YstreamConfig;

import io.debezium.util.Strings;

/**
 * Applies connector-configured options to a YStream builder.
 *
 * <p>YStream exposes options as {@code setXxx} methods. The connector accepts dotted option names and resolves them at
 * runtime, so adding a builder option to the YStream client does not require a corresponding connector code change.</p>
 */
public final class YStreamConfigBuilder {

    private static final Set<String> SENSITIVE_OPTION_MARKERS = Set.of(
            "password", "passwd", "pwd", "passphrase", "secret", "token", "credential", "privatekey", "apikey", "accesskey");

    private YStreamConfigBuilder() {
    }

    /**
     * Applies the given option names and values to the YStream builder and returns it for chaining.
     *
     * <p>Option names are resolved to the builder's single-argument {@code setXxx} methods at runtime and values are
     * converted to the setter's parameter type. Duplicate aliases, empty names, and ambiguous setters are rejected.</p>
     *
     * <p>Callers set the remaining builder options after this method so that dedicated connector settings take
     * precedence over the additional properties.</p>
     *
     * @param builder the YStream builder to configure, must not be null
     * @param properties the option names and values, may be empty
     * @param <T> the record type of the builder
     * @return the configured builder, for method chaining
     * @throws IllegalArgumentException if an option is duplicated, unavailable, or matches multiple setters
     */
    public static <T> YstreamConfig.Builder<T> apply(YstreamConfig.Builder<T> builder, Map<String, String> properties) {
        applyOptions(builder, properties);
        return builder;
    }

    /**
     * Validates and applies additional options to a builder through its single-argument setters.
     *
     * @param builder the target builder
     * @param properties the option names and values, may be empty
     */
    static void applyOptions(Object builder, Map<String, String> properties) {
        Map<String, String> options = normalizeProperties(properties);
        for (Map.Entry<String, String> option : options.entrySet()) {
            apply(builder, option.getKey(), option.getValue());
        }
    }

    /**
     * Validates option names, values, and setters without opening a YStream connection.
     *
     * @param properties the option names and values, may be empty
     */
    public static void validate(Map<String, String> properties) {
        apply(YstreamConfig.builder(), properties);
    }

    private static Map<String, String> normalizeProperties(Map<String, String> properties) {
        Map<String, String> options = new LinkedHashMap<>();
        Map<String, String> names = new LinkedHashMap<>();
        for (Map.Entry<String, String> property : properties.entrySet()) {
            String name = property.getKey();
            if (Strings.isNullOrBlank(name)) {
                throw new IllegalArgumentException("YStream additional property name must not be empty");
            }
            name = name.trim();
            if (isSensitiveOption(name)) {
                throw new IllegalArgumentException("Sensitive YStream builder option '" + name
                        + "' cannot be configured through additional properties");
            }
            if (names.putIfAbsent(normalizedName(name), name) != null) {
                throw new IllegalArgumentException("Duplicate YStream additional property: " + name);
            }
            if (property.getValue() == null) {
                throw new IllegalArgumentException("YStream additional property value must not be null: " + name);
            }
            options.put(name, property.getValue().trim());
        }
        return options;
    }

    private static boolean isSensitiveOption(String name) {
        String normalized = normalizedName(name);
        return SENSITIVE_OPTION_MARKERS.stream().anyMatch(normalized::contains)
                || normalized.endsWith("username") || normalized.endsWith("user");
    }

    private static String normalizedName(String name) {
        return name.replaceAll("[._-]", "").toLowerCase(Locale.ROOT);
    }

    /**
     * Resolves the option's setter by reflection, converts the value to the setter's parameter type, and invokes it.
     *
     * @param builder the target builder instance
     * @param optionName the option name in dotted form, e.g. {@code enable.sequence.next.val}
     * @param value the raw string value to convert
     * @throws IllegalArgumentException if the option is unknown or the value cannot be converted or applied
     */
    private static void apply(Object builder, String optionName, String value) {
        Method setter = findSetter(builder.getClass(), optionName);
        if (setter == null) {
            throw new IllegalArgumentException("Unsupported YStream builder option '" + optionName + "'");
        }
        Object convertedValue;
        try {
            convertedValue = convert(value, setter.getParameterTypes()[0]);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value for YStream builder option '" + optionName
                    + "': " + expectedValue(setter.getParameterTypes()[0]));
        }
        try {
            setter.invoke(builder, convertedValue);
        }
        catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
            // Reflection and setter exceptions may include configured values. Do not expose the cause.
            throw new IllegalArgumentException("Unable to apply YStream builder option '" + optionName + "'");
        }
    }

    private static String expectedValue(Class<?> targetType) {
        if (targetType == boolean.class || targetType == Boolean.class) {
            return "Expected true or false";
        }
        if (targetType == char.class || targetType == Character.class) {
            return "Expected a single character";
        }
        return "Expected a valid " + targetType.getSimpleName() + " value";
    }

    /**
     * Finds the single-argument setter method matching the given option name on the builder class.
     *
     * <p>Separators and case are ignored when matching the option to a setter, so {@code enable.sequence.next.val}
     * matches {@code setEnableSequenceNextVal}. Multiple matching setters are rejected regardless of their parameter
     * types or the order returned by reflection. Compiler-generated bridge and synthetic methods are ignored.</p>
     *
     * @param builderClass the builder class to search, including inherited public methods
     * @param optionName the option name in dotted form
     * @return the matching single-argument setter, or null if none exists
     * @throws IllegalArgumentException if multiple setters match the option name
     */
    private static Method findSetter(Class<?> builderClass, String optionName) {
        String normalizedOption = normalizedName(optionName);
        List<Method> matches = new ArrayList<>();
        for (Method method : builderClass.getMethods()) {
            if (method.getName().startsWith("set") && method.getParameterCount() == 1
                    && !method.isBridge() && !method.isSynthetic()
                    && normalizedName(method.getName().substring(3)).equals(normalizedOption)) {
                matches.add(method);
            }
        }
        if (matches.isEmpty()) {
            return null;
        }
        if (matches.size() > 1) {
            throw new IllegalArgumentException("Ambiguous YStream builder option '" + optionName
                    + "': multiple matching setters");
        }
        return matches.get(0);
    }

    /**
     * Converts the string value to the setter's target parameter type.
     *
     * <p>Supports strings, primitive wrappers, single-character values, and enum constants (matched case-insensitively);
     * conversion failures raise an {@link IllegalArgumentException} that surfaces to the connector configuration.</p>
     *
     * @param value the raw string value
     * @param targetType the setter's parameter type
     * @return the converted value
     * @throws IllegalArgumentException if the value has an invalid format or the target type is unsupported
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static Object convert(String value, Class<?> targetType) {
        if (targetType == String.class) {
            return value;
        }
        if (targetType == boolean.class || targetType == Boolean.class) {
            if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                throw new IllegalArgumentException("Expected true or false");
            }
            return Boolean.parseBoolean(value);
        }
        if (targetType == int.class || targetType == Integer.class) {
            return Integer.parseInt(value);
        }
        if (targetType == long.class || targetType == Long.class) {
            return Long.parseLong(value);
        }
        if (targetType == short.class || targetType == Short.class) {
            return Short.parseShort(value);
        }
        if (targetType == byte.class || targetType == Byte.class) {
            return Byte.parseByte(value);
        }
        if (targetType == double.class || targetType == Double.class) {
            return Double.parseDouble(value);
        }
        if (targetType == float.class || targetType == Float.class) {
            return Float.parseFloat(value);
        }
        if (targetType == char.class || targetType == Character.class) {
            if (value.length() != 1) {
                throw new IllegalArgumentException("Expected a single character");
            }
            return value.charAt(0);
        }
        if (targetType.isEnum()) {
            return Enum.valueOf((Class<? extends Enum>) targetType, value.toUpperCase(Locale.ROOT));
        }
        throw new IllegalArgumentException("Unsupported YStream builder option type: " + targetType.getName());
    }
}
