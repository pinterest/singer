/**
 * Copyright 2019 Pinterest, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.singer.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Cache for compiled regex patterns to avoid repeated Pattern.compile() calls.
 * This significantly improves performance when the same regex patterns are used frequently.
 */
public class PatternCache {

    private static final Logger LOG = LoggerFactory.getLogger(PatternCache.class);

    private static final ConcurrentMap<String, Pattern> PATTERN_CACHE = new ConcurrentHashMap<>();

    /**
     * Gets a compiled Pattern for the given regex string with flags.
     * Uses caching to avoid repeated compilation of the same pattern.
     *
     * @param regex The regex string to compile
     * @param flags The regex flags (e.g., Pattern.UNIX_LINES, Pattern.DOTALL)
     * @return The compiled Pattern object
     * @throws PatternSyntaxException if the regex is invalid
     */
    public static Pattern getPattern(String regex, int flags) throws PatternSyntaxException {
        if (regex == null) {
            throw new IllegalArgumentException("Regex pattern cannot be null");
        }

        // Create cache key that includes flags to ensure different flag combinations get separate entries
        String cacheKey = regex + "|flags=" + flags;

        return PATTERN_CACHE.computeIfAbsent(cacheKey, regexKey -> {
            LOG.info("Singer regex optimization: Compiling new pattern with flags: {} (flags={})", regex, flags);
            return Pattern.compile(regex, flags);
        });
    }

    /**
     * Gets a compiled Pattern for the given regex string.
     * Uses caching to avoid repeated compilation of the same pattern.
     *
     * @param regex The regex string to compile
     * @return The compiled Pattern object
     * @throws PatternSyntaxException if the regex is invalid
     */
    public static Pattern getPattern(String regex) throws PatternSyntaxException {
        return getPattern(regex, Pattern.UNIX_LINES);
    }

    /**
     * Gets the number of cached patterns (for monitoring/testing)
     */
    public static int getCacheSize() {
        return PATTERN_CACHE.size();
    }

    /**
     * Clears the cache (primarily for testing)
     */
    public static void clearCache() {
        PATTERN_CACHE.clear();
        LOG.warn("Singer regex optimization: Pattern cache cleared");
    }
}