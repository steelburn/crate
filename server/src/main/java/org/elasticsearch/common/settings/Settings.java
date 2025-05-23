/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import static io.crate.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.common.unit.ByteSizeValue.parseBytesSizeValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.jetbrains.annotations.Nullable;

import ch.randelshofer.fastdoubleparser.JavaDoubleParser;
import ch.randelshofer.fastdoubleparser.JavaFloatParser;
import io.crate.common.Booleans;
import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;
import io.crate.server.xcontent.LoggingDeprecationHandler;
import io.crate.server.xcontent.XContentParserUtils;
import io.crate.sql.tree.GenericProperties;

/**
 * An immutable settings implementation.
 */
public final class Settings {

    public static final Settings EMPTY = new Builder().build();

    public static final String MASKED_VALUE = "[xxxxx]";

    /** The raw settings from the full key to raw string value. */
    private final Map<String, Object> settings;

    /** The first level of setting names. This is constructed lazily in {@link #names()}. */
    private final SetOnce<Set<String>> firstLevelNames = new SetOnce<>();

    /**
     * Setting names found in this Settings.
     * This is constructed lazily in {@link #keySet()}.
     */
    private final SetOnce<Set<String>> keys = new SetOnce<>();

    Settings(Map<String, Object> settings) {
        // we use a sorted map for consistent serialization when using getAsMap()
        this.settings = Collections.unmodifiableSortedMap(new TreeMap<>(settings));
    }

    public Map<String, Object> getAsStructuredMap() {
        return getAsStructuredMap(Set.of());
    }

    public Map<String, Object> getAsStructuredMap(Set<String> maskedSettings) {
        Map<String, Object> map = new HashMap<>(2);
        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            processSetting(map, "", entry.getKey(), entry.getValue(), maskedSettings);
        }
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked") Map<String, Object> valMap = (Map<String, Object>) entry.getValue();
                entry.setValue(convertMapsToArrays(valMap));
            }
        }

        return map;
    }

    private void processSetting(Map<String, Object> map,
                                String prefix,
                                String setting,
                                Object value,
                                Set<String> maskedSettings) {
        int prefixLength = setting.indexOf('.');
        if (prefixLength == -1) {
            @SuppressWarnings("unchecked") Map<String, Object> innerMap = (Map<String, Object>) map.get(prefix + setting);
            if (innerMap != null) {
                // It supposed to be a value, but we already have a map stored, need to convert this map to "." notation
                for (Map.Entry<String, Object> entry : innerMap.entrySet()) {
                    map.put(prefix + setting + "." + entry.getKey(), entry.getValue());
                }
            }
            String settingKey = prefix + setting;
            Object settingValue = maskedSettings.contains(settingKey) ? MASKED_VALUE : value;
            map.put(settingKey, settingValue);
        } else {
            String key = setting.substring(0, prefixLength);
            String rest = setting.substring(prefixLength + 1);
            Object existingValue = map.get(prefix + key);
            if (existingValue == null) {
                Map<String, Object> newMap = new HashMap<>(2);
                processSetting(newMap, "", rest, value, maskedSettings);
                map.put(key, newMap);
            } else {
                if (existingValue instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> innerMap = (Map<String, Object>) existingValue;
                    processSetting(innerMap, "", rest, value, maskedSettings);
                    map.put(key, innerMap);
                } else {
                    // It supposed to be a map, but we already have a value stored, which is not a map
                    // fall back to "." notation
                    processSetting(map, prefix + key + ".", rest, value, maskedSettings);
                }
            }
        }
    }

    private Object convertMapsToArrays(Map<String, Object> map) {
        if (map.isEmpty()) {
            return map;
        }
        boolean isArray = true;
        int maxIndex = -1;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (isArray) {
                try {
                    int index = Integer.parseInt(entry.getKey());
                    if (index >= 0) {
                        maxIndex = Math.max(maxIndex, index);
                    } else {
                        isArray = false;
                    }
                } catch (NumberFormatException ex) {
                    isArray = false;
                }
            }
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked") Map<String, Object> valMap = (Map<String, Object>) entry.getValue();
                entry.setValue(convertMapsToArrays(valMap));
            }
        }
        if (isArray && (maxIndex + 1) == map.size()) {
            ArrayList<Object> newValue = new ArrayList<>(maxIndex + 1);
            for (int i = 0; i <= maxIndex; i++) {
                Object obj = map.get(Integer.toString(i));
                if (obj == null) {
                    // Something went wrong. Different format?
                    // Bailout!
                    return map;
                }
                newValue.add(obj);
            }
            return newValue;
        }
        return map;
    }

    /**
     * A settings that are filtered (and key is removed) with the specified prefix.
     */
    public Settings getByPrefix(String prefix) {
        return new Settings(new FilteredMap(this.settings, (k) -> k.startsWith(prefix), prefix));
    }

    /**
     * Returns a new settings object that contains all setting of the current one filtered by the given settings key predicate.
     */
    public Settings filter(Predicate<String> predicate) {
        return new Settings(new FilteredMap(this.settings, predicate, null));
    }

    /**
     * Returns the settings mapped to the given setting name.
     */
    public Settings getAsSettings(String setting) {
        return getByPrefix(setting + ".");
    }

    /**
     * Returns the setting value associated with the setting key.
     *
     * @param setting The setting key
     * @return The setting value, {@code null} if it does not exists.
     */
    public String get(String setting) {
        return toString(settings.get(setting));
    }

    /**
     * Returns the setting value associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    public String get(String setting, String defaultValue) {
        String retVal = get(setting);
        return retVal == null ? defaultValue : retVal;
    }

    /**
     * Returns the setting value (as float) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    public Float getAsFloat(String setting, Float defaultValue) {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return JavaFloatParser.parseFloat(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse float setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    /**
     * Returns the setting value (as double) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    public Double getAsDouble(String setting, Double defaultValue) {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return JavaDoubleParser.parseDouble(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse double setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    /**
     * Returns the setting value (as int) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    public Integer getAsInt(String setting, Integer defaultValue) {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse int setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    /**
     * Returns the setting value (as long) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    public Long getAsLong(String setting, Long defaultValue) {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse long setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    /**
     * Returns <code>true</code> iff the given key has a value in this settings object
     */
    public boolean hasValue(String key) {
        return settings.get(key) != null;
    }

    /**
     * We have to lazy initialize the deprecation logger as otherwise a static logger here would be constructed before logging is configured
     * leading to a runtime failure (see {@link LogConfigurator#checkErrorListener()} ). The premature construction would come from any
     * {@link Setting} object constructed in, for example, {@link org.elasticsearch.env.Environment}.
     */
    static class DeprecationLoggerHolder {
        static DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(Settings.class));
    }

    /**
     * Returns the setting value (as boolean) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    public Boolean getAsBoolean(String setting, Boolean defaultValue) {
        return Booleans.parseBoolean(get(setting), defaultValue);
    }

    /**
     * Returns the setting value (as time) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    public TimeValue getAsTime(String setting, TimeValue defaultValue) {
        return parseTimeValue(get(setting), defaultValue, setting);
    }

    /**
     * Returns the setting value (as size) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    public ByteSizeValue getAsBytesSize(String setting, ByteSizeValue defaultValue) throws SettingsException {
        return parseBytesSizeValue(get(setting), defaultValue, setting);
    }

    /**
     * The values associated with a setting key as an immutable list.
     * <p>
     * It will also automatically load a comma separated list under the settingPrefix and merge with
     * the numbered format.
     *
     * @param key The setting key to load the list by
     * @return The setting list values
     */
    public List<String> getAsList(String key) throws SettingsException {
        return getAsList(key, Collections.emptyList());
    }

    /**
     * The values associated with a setting key as an immutable list.
     * <p>
     * If commaDelimited is true, it will automatically load a comma separated list under the settingPrefix and merge with
     * the numbered format.
     *
     * @param key The setting key to load the list by
     * @return The setting list values
     */
    public List<String> getAsList(String key, List<String> defaultValue) throws SettingsException {
        return getAsList(key, defaultValue, true);
    }

    /**
     * The values associated with a setting key as an immutable list.
     * <p>
     * It will also automatically load a comma separated list under the settingPrefix and merge with
     * the numbered format.
     *
     * @param key  The setting key to load the list by
     * @param defaultValue   The default value to use if no value is specified
     * @param commaDelimited Whether to try to parse a string as a comma-delimited value
     * @return The setting list values
     */
    @SuppressWarnings("unchecked")
    public List<String> getAsList(String key, List<String> defaultValue, Boolean commaDelimited) throws SettingsException {
        List<String> result = new ArrayList<>();
        final Object valueFromPrefix = settings.get(key);
        if (valueFromPrefix != null) {
            if (valueFromPrefix instanceof List) {
                return Collections.unmodifiableList((List<String>) valueFromPrefix);
            } else if (commaDelimited) {
                String[] strings = Strings.splitStringByCommaToArray(get(key));
                for (String string : strings) {
                    result.add(string.trim());
                }
            } else {
                result.add(get(key).trim());
            }
        }

        if (result.isEmpty()) {
            return defaultValue;
        }
        return Collections.unmodifiableList(result);
    }



    /**
     * Returns group settings for the given setting prefix.
     */
    public Map<String, Settings> getGroups(String settingPrefix) throws SettingsException {
        return getGroups(settingPrefix, false);
    }

    /**
     * Returns group settings for the given setting prefix.
     */
    public Map<String, Settings> getGroups(String settingPrefix, boolean ignoreNonGrouped) throws SettingsException {
        if (!Strings.hasLength(settingPrefix)) {
            throw new IllegalArgumentException("illegal setting prefix " + settingPrefix);
        }
        if (settingPrefix.charAt(settingPrefix.length() - 1) != '.') {
            settingPrefix = settingPrefix + ".";
        }
        return getGroupsInternal(settingPrefix, ignoreNonGrouped);
    }

    private Map<String, Settings> getGroupsInternal(String settingPrefix, boolean ignoreNonGrouped) throws SettingsException {
        Settings prefixSettings = getByPrefix(settingPrefix);
        Map<String, Settings> groups = new HashMap<>();
        for (String groupName : prefixSettings.names()) {
            Settings groupSettings = prefixSettings.getByPrefix(groupName + ".");
            if (groupSettings.isEmpty()) {
                if (ignoreNonGrouped) {
                    continue;
                }
                throw new SettingsException("Failed to get setting group for [" + settingPrefix + "] setting prefix and setting ["
                    + settingPrefix + groupName + "] because of a missing '.'");
            }
            groups.put(groupName, groupSettings);
        }

        return Collections.unmodifiableMap(groups);
    }

    /**
     * Returns group settings for the given setting prefix.
     */
    public Map<String, Settings> getAsGroups() throws SettingsException {
        return getAsGroups(false);
    }

    public Map<String, Settings> getAsGroups(boolean ignoreNonGrouped) throws SettingsException {
        return getGroupsInternal("", ignoreNonGrouped);
    }

    /**
     * Returns a parsed version.
     */
    public Version getAsVersion(String setting, Version defaultVersion) throws SettingsException {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultVersion;
        }
        try {
            return Version.fromId(Integer.parseInt(sValue));
        } catch (Exception e) {
            throw new SettingsException("Failed to parse version setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    /**
     * @return  The direct keys of this settings
     */
    public Set<String> names() {
        synchronized (firstLevelNames) {
            if (firstLevelNames.get() == null) {
                Set<String> names = settings.keySet().stream().map(k -> {
                    final int i = k.indexOf('.');
                    return i < 0 ? k : k.substring(0, i);
                }).collect(Collectors.toSet());
                firstLevelNames.set(Collections.unmodifiableSet(names));
            }
        }
        return firstLevelNames.get();
    }

    /**
     * Returns the settings as delimited string.
     */
    public String toDelimitedString(char delimiter) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(delimiter);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Settings that = (Settings) o;
        return Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return settings != null ? settings.hashCode() : 0;
    }

    @SuppressWarnings("unchecked")
    public static Settings readSettingsFromStream(StreamInput in) throws IOException {
        Builder builder = new Builder();
        int numberOfSettings = in.readVInt();
        for (int i = 0; i < numberOfSettings; i++) {
            String key = in.readString();
            Object value = in.readGenericValue();
            if (value == null) {
                builder.putNull(key);
            } else if (value instanceof List) {
                builder.putList(key, (List<String>) value);
            } else {
                builder.put(key, value.toString());
            }
        }
        return builder.build();
    }

    public static void writeSettingsToStream(StreamOutput out, Settings settings) throws IOException {
        Set<Map.Entry<String, Object>> entries = settings.settings.entrySet();
        out.writeVInt(entries.size());
        for (Map.Entry<String, Object> entry : entries) {
            out.writeString(entry.getKey());
            out.writeGenericValue(entry.getValue());
        }
    }

    /**
     * Returns a builder to be used in order to build settings.
     */
    public static Builder builder() {
        return new Builder();
    }

    public XContentBuilder toXContent(XContentBuilder builder, boolean flat) throws IOException {
        if (flat) {
            for (Map.Entry<String, Object> entry : settings.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        } else {
            for (Map.Entry<String, Object> entry : getAsStructuredMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }
        return builder;
    }

    /**
     * Parsers the generated xcontent from {@link Settings#toXContent(XContentBuilder, Params)} into a new Settings object.
     * Note this method requires the parser to either be positioned on a null token or on
     * {@link org.elasticsearch.common.xcontent.XContentParser.Token#START_OBJECT}.
     */
    public static Settings fromXContent(XContentParser parser) throws IOException {
        return fromXContent(parser, true, false);
    }

    private static Settings fromXContent(XContentParser parser, boolean allowNullValues, boolean validateEndOfStream) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        Builder innerBuilder = Settings.builder();
        StringBuilder currentKeyBuilder = new StringBuilder();
        fromXContent(parser, currentKeyBuilder, innerBuilder, allowNullValues);
        if (validateEndOfStream) {
            // ensure we reached the end of the stream
            XContentParser.Token lastToken = null;
            try {
                while (!parser.isClosed() && (lastToken = parser.nextToken()) == null) ;
            } catch (Exception e) {
                throw new ElasticsearchParseException(
                    "malformed, expected end of settings but encountered additional content starting at line number: [{}], "
                        + "column number: [{}]",
                    e, parser.getTokenLocation().lineNumber, parser.getTokenLocation().columnNumber);
            }
            if (lastToken != null) {
                throw new ElasticsearchParseException(
                    "malformed, expected end of settings but encountered additional content starting at line number: [{}], "
                        + "column number: [{}]",
                    parser.getTokenLocation().lineNumber, parser.getTokenLocation().columnNumber);
            }
        }
        return innerBuilder.build();
    }

    private static void fromXContent(XContentParser parser, StringBuilder keyBuilder, Settings.Builder builder,
                                     boolean allowNullValues) throws IOException {
        final int length = keyBuilder.length();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                keyBuilder.setLength(length);
                keyBuilder.append(parser.currentName());
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                keyBuilder.append('.');
                fromXContent(parser, keyBuilder, builder, allowNullValues);
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                List<String> list = new ArrayList<>();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                        list.add(parser.text());
                    } else if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                        list.add(parser.text()); // just use the string representation here
                    } else if (parser.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                        list.add(String.valueOf(parser.text()));
                    } else {
                        throw new IllegalStateException("only value lists are allowed in serialized settings");
                    }
                }
                String key = keyBuilder.toString();
                validateValue(key, list, builder, parser, allowNullValues);
                builder.putList(key, list);
            } else if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                String key = keyBuilder.toString();
                validateValue(key, null, builder, parser, allowNullValues);
                builder.putNull(key);
            } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING
                || parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                String key = keyBuilder.toString();
                String value = parser.text();
                validateValue(key, value, builder, parser, allowNullValues);
                builder.put(key, value);
            } else if (parser.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                String key = keyBuilder.toString();
                validateValue(key, parser.text(), builder, parser, allowNullValues);
                builder.put(key, parser.booleanValue());
            } else {
                XContentParserUtils.throwUnknownToken(parser.currentToken(), parser.getTokenLocation());
            }
        }
    }

    private static void validateValue(String key, Object currentValue, Settings.Builder builder, XContentParser parser,
                                      boolean allowNullValues) {
        if (builder.map.containsKey(key)) {
            throw new ElasticsearchParseException(
                "duplicate settings key [{}] found at line number [{}], column number [{}], previous value [{}], current value [{}]",
                key,
                parser.getTokenLocation().lineNumber,
                parser.getTokenLocation().columnNumber,
                builder.map.get(key),
                currentValue
            );
        }

        if (currentValue == null && allowNullValues == false) {
            throw new ElasticsearchParseException(
                "null-valued setting found for key [{}] found at line number [{}], column number [{}]",
                key,
                parser.getTokenLocation().lineNumber,
                parser.getTokenLocation().columnNumber
            );
        }
    }


    /**
     * Returns {@code true} if this settings object contains no settings
     * @return {@code true} if this settings object contains no settings
     */
    public boolean isEmpty() {
        return this.settings.isEmpty();
    }

    /** Returns the number of settings in this settings object. */
    public int size() {
        return keySet().size();
    }

    /** Returns the fully qualified setting names contained in this settings object. */
    public Set<String> keySet() {
        synchronized (keys) {
            if (keys.get() == null) {
                keys.set(settings.keySet());
            }
        }
        return keys.get();
    }

    /**
     * A builder allowing to put different settings and then {@link #build()} an immutable
     * settings implementation. Use {@link Settings#builder()} in order to
     * construct it.
     */
    public static class Builder {

        // we use a sorted map for consistent serialization when using getAsMap()
        private final Map<String, Object> map = new TreeMap<>();

        private Builder() {
        }

        public Set<String> keys() {
            return this.map.keySet();
        }

        /**
         * Removes the provided setting from the internal map holding the current list of settings.
         */
        public String remove(String key) {
            return Settings.toString(map.remove(key));
        }

        /**
         * Returns a setting value based on the setting key.
         */
        public String get(String key) {
            return Settings.toString(map.get(key));
        }

        /**
         * Sets a path setting with the provided setting key and path.
         *
         * @param key  The setting key
         * @param path The setting path
         * @return The builder
         */
        public Builder put(String key, Path path) {
            return put(key, path.toString());
        }

        /**
         * Sets a time value setting with the provided setting key and value.
         *
         * @param key  The setting key
         * @param timeValue The setting timeValue
         * @return The builder
         */
        public Builder put(String key, TimeValue timeValue) {
            return put(key, timeValue.toString());
        }

        /**
         * Sets a byteSizeValue setting with the provided setting key and byteSizeValue.
         *
         * @param key  The setting key
         * @param byteSizeValue The setting value
         * @return The builder
         */
        public Builder put(String key, ByteSizeValue byteSizeValue) {
            return put(key, byteSizeValue.toString());
        }

        /**
         * Sets an enum setting with the provided setting key and enum instance.
         *
         * @param key  The setting key
         * @param enumValue The setting value
         * @return The builder
         */
        public Builder put(String key, Enum<?> enumValue) {
            return put(key, enumValue.toString());
        }

        /**
         * Sets an level setting with the provided setting key and level instance.
         *
         * @param key  The setting key
         * @param level The setting value
         * @return The builder
         */
        public Builder put(String key, Level level) {
            return put(key, level.toString());
        }

        /**
         * Sets an lucene version setting with the provided setting key and lucene version instance.
         *
         * @param key  The setting key
         * @param luceneVersion The setting value
         * @return The builder
         */
        public Builder put(String key, org.apache.lucene.util.Version luceneVersion) {
            return put(key, luceneVersion.toString());
        }

        /**
         * Sets a setting with the provided setting key and value.
         *
         * @param key   The setting key
         * @param value The setting value
         * @return The builder
         */
        public Builder put(String key, String value) {
            map.put(key, value);
            return this;
        }

        /*
         * Sets a value as list if it is an array or list. Otherwise as string via value.toString()
         */
        public Builder putStringOrList(String key, @Nullable Object value) {
            if (value instanceof String[] strings) {
                return putList(key, strings);
            } else if (value instanceof List<?> list) {
                return putList(key, list);
            } else {
                return put(key, value == null ? null : value.toString());
            }
        }

        public Builder copy(String key, Settings source) {
            return copy(key, key, source);
        }

        public Builder copy(String key, String sourceKey, Settings source) {
            if (source.settings.containsKey(sourceKey) == false) {
                throw new IllegalArgumentException("source key not found in the source settings");
            }
            final Object value = source.settings.get(sourceKey);
            if (value instanceof List<?> list) {
                return putList(key, list);
            } else if (value == null) {
                return putNull(key);
            } else {
                return put(key, Settings.toString(value));
            }
        }

        /**
         * Sets a null value for the given setting key
         */
        public Builder putNull(String key) {
            return put(key, (String) null);
        }

        /**
         * Sets a setting with the provided setting key and class as value.
         *
         * @param key   The setting key
         * @param clazz The setting class value
         * @return The builder
         */
        public Builder put(String key, Class<?> clazz) {
            map.put(key, clazz.getName());
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the boolean value.
         *
         * @param setting The setting key
         * @param value   The boolean value
         * @return The builder
         */
        public Builder put(String setting, boolean value) {
            put(setting, String.valueOf(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the int value.
         *
         * @param setting The setting key
         * @param value   The int value
         * @return The builder
         */
        public Builder put(String setting, int value) {
            put(setting, String.valueOf(value));
            return this;
        }

        public Builder put(String setting, Object value) {
            switch (value) {
                case Integer i -> put(setting, i.intValue());
                case Long l -> put(setting, l.longValue());
                case String l -> put(setting, l);
                case Float f -> put(setting, f.floatValue());
                case Double d -> put(setting, d.doubleValue());
                case Boolean b -> put(setting, b.booleanValue());
                case List<?> l -> putList(setting, l);
                default -> throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Unsupported value: `%s`",
                    setting,
                    value
                ));
            }
            return this;
        }

        public Builder put(GenericProperties<Object> properties) {
            for (Entry<String,Object> entry : properties) {
                String settingName = entry.getKey();
                putStringOrList(settingName, entry.getValue());
            }
            return this;
        }

        public Builder put(String setting, Version version) {
            put(setting, version.internalId);
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the long value.
         *
         * @param setting The setting key
         * @param value   The long value
         * @return The builder
         */
        public Builder put(String setting, long value) {
            put(setting, String.valueOf(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the float value.
         *
         * @param setting The setting key
         * @param value   The float value
         * @return The builder
         */
        public Builder put(String setting, float value) {
            put(setting, String.valueOf(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the double value.
         *
         * @param setting The setting key
         * @param value   The double value
         * @return The builder
         */
        public Builder put(String setting, double value) {
            put(setting, String.valueOf(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the time value.
         *
         * @param setting The setting key
         * @param value   The time value
         * @return The builder
         */
        public Builder put(String setting, long value, TimeUnit timeUnit) {
            put(setting, timeUnit.toMillis(value) + "ms");
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the size value.
         *
         * @param setting The setting key
         * @param value   The size value
         * @return The builder
         */
        public Builder put(String setting, long value, ByteSizeUnit sizeUnit) {
            put(setting, sizeUnit.toBytes(value) + "b");
            return this;
        }


        /**
         * Sets the setting with the provided setting key and an array of values.
         *
         * @param setting The setting key
         * @param values  The values
         * @return The builder
         */
        public Builder putList(String setting, String... values) {
            return putList(setting, Arrays.asList(values));
        }

        /**
         * Sets the setting with the provided setting key and a list of values.
         *
         * @param setting The setting key
         * @param values  The values
         * @return The builder
         */
        public Builder putList(String setting, List<?> values) {
            remove(setting);
            map.put(setting, new ArrayList<>(values));
            return this;
        }

        /**
         * Sets the setting group.
         */
        public Builder put(String settingPrefix, String groupName, String[] settings, String[] values) throws SettingsException {
            if (settings.length != values.length) {
                throw new SettingsException("The settings length must match the value length");
            }
            for (int i = 0; i < settings.length; i++) {
                if (values[i] == null) {
                    continue;
                }
                put(settingPrefix + "." + groupName + "." + settings[i], values[i]);
            }
            return this;
        }

        /**
         * Sets all the provided settings.
         * @param settings the settings to set
         */
        public Builder put(Settings settings) {
            Map<String, Object> settingsMap = new HashMap<>(settings.settings);
            processLegacyLists(settingsMap);
            map.putAll(settingsMap);
            return this;
        }

        private void processLegacyLists(Map<String, Object> map) {
            String[] array = map.keySet().toArray(new String[0]);
            for (String key : array) {
                if (key.endsWith(".0")) { // let's only look at the head of the list and convert in order starting there.
                    int counter = 0;
                    String prefix = key.substring(0, key.lastIndexOf('.'));
                    if (map.containsKey(prefix)) {
                        throw new IllegalStateException("settings builder can't contain values for [" + prefix + "=" + map.get(prefix)
                            + "] and [" + key + "=" + map.get(key) + "]");
                    }
                    List<String> values = new ArrayList<>();
                    while (true) {
                        String listKey = prefix + '.' + (counter++);
                        String value = get(listKey);
                        if (value == null) {
                            map.put(prefix, values);
                            break;
                        } else {
                            values.add(value);
                            map.remove(listKey);
                        }
                    }
                }
            }
        }

        /**
         * Loads settings from the actual string content that represents them using {@link #fromXContent(XContentParser)}
         */
        public Builder loadFromSource(String source, XContentType xContentType) {
            try (XContentParser parser = xContentType.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, source)) {
                this.put(fromXContent(parser, true, true));
            } catch (Exception e) {
                throw new SettingsException("Failed to load settings from [" + source + "]", e);
            }
            return this;
        }

        /**
         * Loads settings from a url that represents them using {@link #fromXContent(XContentParser)}
         * Note: Loading from a path doesn't allow <code>null</code> values in the incoming xcontent
         */
        public Builder loadFromPath(Path path) throws IOException {
            // NOTE: loadFromStream will close the input stream
            return loadFromStream(path.getFileName().toString(), Files.newInputStream(path), false);
        }

        /**
         * Loads settings from a stream that represents them using {@link #fromXContent(XContentParser)}
         */
        public Builder loadFromStream(String resourceName, InputStream is, boolean acceptNullValues) throws IOException {
            final XContentType xContentType;
            if (resourceName.endsWith(".json")) {
                xContentType = XContentType.JSON;
            } else if (resourceName.endsWith(".yml") || resourceName.endsWith(".yaml")) {
                xContentType = XContentType.YAML;
            } else {
                throw new IllegalArgumentException("unable to detect content type from resource name [" + resourceName + "]");
            }
            // fromXContent doesn't use named xcontent or deprecation.
            try (XContentParser parser = xContentType.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, is)) {
                if (parser.currentToken() == null && parser.nextToken() == null) {
                    return this; // empty file
                }
                put(fromXContent(parser, acceptNullValues, true));
            } catch (ElasticsearchParseException e) {
                throw e;
            } catch (Exception e) {
                throw new SettingsException("Failed to load settings from [" + resourceName + "]", e);
            } finally {
                IOUtils.close(is);
            }
            return this;
        }

        public Builder putProperties(final Map<String, String> esSettings, final UnaryOperator<String> keyFunction) {
            for (final Map.Entry<String, String> esSetting : esSettings.entrySet()) {
                final String key = esSetting.getKey();
                put(keyFunction.apply(key), esSetting.getValue());
            }
            return this;
        }

        /**
         * Runs across all the settings set on this builder and
         * replaces {@code ${...}} elements in each setting with
         * another setting already set on this builder.
         */
        public Builder replacePropertyPlaceholders() {
            return replacePropertyPlaceholders(System::getenv);
        }

        // visible for testing
        Builder replacePropertyPlaceholders(UnaryOperator<String> getenv) {
            PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
            PropertyPlaceholder.PlaceholderResolver placeholderResolver = new PropertyPlaceholder.PlaceholderResolver() {
                @Override
                public String resolvePlaceholder(String placeholderName) {
                    final String value = getenv.apply(placeholderName);
                    if (value != null) {
                        return value;
                    }
                    return Settings.toString(map.get(placeholderName));
                }

                @Override
                public boolean shouldIgnoreMissing(String placeholderName) {
                    return placeholderName.startsWith("prompt.");
                }

                @Override
                public boolean shouldRemoveMissingPlaceholder(String placeholderName) {
                    return !placeholderName.startsWith("prompt.");
                }
            };

            Iterator<Map.Entry<String, Object>> entryItr = map.entrySet().iterator();
            while (entryItr.hasNext()) {
                Map.Entry<String, Object> entry = entryItr.next();
                if (entry.getValue() == null) {
                    // a null value obviously can't be replaced
                    continue;
                }
                if (entry.getValue() instanceof List) {
                    @SuppressWarnings("unchecked")
                    final ListIterator<String> li = ((List<String>) entry.getValue()).listIterator();
                    while (li.hasNext()) {
                        final String settingValueRaw = li.next();
                        final String settingValueResolved = propertyPlaceholder.replacePlaceholders(settingValueRaw, placeholderResolver);
                        li.set(settingValueResolved);
                    }
                    continue;
                }

                String value = propertyPlaceholder.replacePlaceholders(Settings.toString(entry.getValue()), placeholderResolver);
                // if the values exists and has length, we should maintain it  in the map
                // otherwise, the replace process resolved into removing it
                if (Strings.hasLength(value)) {
                    entry.setValue(value);
                } else {
                    entryItr.remove();
                }
            }
            return this;
        }

        /**
         * Checks that all settings in the builder start with the specified prefix.
         *
         * If a setting doesn't start with the prefix, the builder appends the prefix to such setting.
         */
        public Builder normalizePrefix(String prefix) {
            Map<String, Object> replacements = new HashMap<>();
            Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                String key = entry.getKey();
                if (key.startsWith(prefix) == false && key.endsWith("*") == false) {
                    replacements.put(prefix + key, entry.getValue());
                    iterator.remove();
                }
            }
            map.putAll(replacements);
            return this;
        }

        /**
         * Builds a {@link Settings} (underlying uses {@link Settings}) based on everything
         * set on this builder.
         */
        public Settings build() {
            processLegacyLists(map);
            return new Settings(map);
        }
    }

    // TODO We could use an FST internally to make things even faster and more compact
    private static final class FilteredMap extends AbstractMap<String, Object> {
        private final Map<String, Object> delegate;
        private final Predicate<String> filter;
        private final String prefix;
        // we cache that size since we have to iterate the entire set
        // this is safe to do since this map is only used with unmodifiable maps
        private int size = -1;

        @Override
        public Set<Entry<String, Object>> entrySet() {
            Set<Entry<String, Object>> delegateSet = delegate.entrySet();
            return new AbstractSet<>() {

                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    Iterator<Entry<String, Object>> iter = delegateSet.iterator();

                    return new Iterator<Entry<String, Object>>() {
                        private int numIterated;
                        private Entry<String, Object> currentElement;

                        @Override
                        public boolean hasNext() {
                            if (currentElement != null) {
                                return true; // protect against calling hasNext twice
                            } else {
                                if (numIterated == size) { // early terminate
                                    assert size != -1 : "size was never set: " + numIterated + " vs. " + size;
                                    return false;
                                }
                                while (iter.hasNext()) {
                                    if (filter.test((currentElement = iter.next()).getKey())) {
                                        numIterated++;
                                        return true;
                                    }
                                }
                                // we didn't find anything
                                currentElement = null;
                                return false;
                            }
                        }

                        @Override
                        public Entry<String, Object> next() {
                            if (currentElement == null &&
                                hasNext() == false) { // protect against no #hasNext call or not respecting it

                                throw new NoSuchElementException("make sure to call hasNext first");
                            }
                            final Entry<String, Object> current = this.currentElement;
                            this.currentElement = null;
                            if (prefix == null) {
                                return current;
                            }
                            return new Entry<String, Object>() {
                                @Override
                                public String getKey() {
                                    return current.getKey().substring(prefix.length());
                                }

                                @Override
                                public Object getValue() {
                                    return current.getValue();
                                }

                                @Override
                                public Object setValue(Object value) {
                                    throw new UnsupportedOperationException();
                                }
                            };
                        }
                    };
                }

                @Override
                public int size() {
                    return FilteredMap.this.size();
                }
            };
        }

        private FilteredMap(Map<String, Object> delegate, Predicate<String> filter, String prefix) {
            this.delegate = delegate;
            this.filter = filter;
            this.prefix = prefix;
        }

        @Override
        public Object get(Object key) {
            if (key instanceof String str) {
                final String theKey = prefix == null ? str : prefix + str;
                if (filter.test(theKey)) {
                    return delegate.get(theKey);
                }
            }
            return null;
        }

        @Override
        public boolean containsKey(Object key) {
            if (key instanceof String str) {
                final String theKey = prefix == null ? str : prefix + str;
                if (filter.test(theKey)) {
                    return delegate.containsKey(theKey);
                }
            }
            return false;
        }

        @Override
        public int size() {
            if (size == -1) {
                size = Math.toIntExact(delegate.keySet().stream().filter(filter).count());
            }
            return size;
        }
    }

    @Override
    public String toString() {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject();
            toXContent(builder, true);
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String toString(Object o) {
        return o == null ? null : o.toString();
    }

}
