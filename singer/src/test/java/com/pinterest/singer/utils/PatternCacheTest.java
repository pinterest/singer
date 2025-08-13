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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class PatternCacheTest {

    @Before
    public void setUp() {
        // Clear cache before each test to ensure clean state
        PatternCache.clearCache();
    }

    @After
    public void tearDown() {
        // Clear cache after each test
        PatternCache.clearCache();
    }

    @Test
    public void testBasicPatternCaching() {
        String regex = "^.*$";

        // First call should compile and cache
        Pattern pattern1 = PatternCache.getPattern(regex);
        assertEquals(1, PatternCache.getCacheSize());

        // Second call should return cached pattern (same instance)
        Pattern pattern2 = PatternCache.getPattern(regex);
        assertEquals(1, PatternCache.getCacheSize());
        assertSame("Should return same cached pattern instance", pattern1, pattern2);
    }

    @Test
    public void testPatternWithFlags() {
        String regex = "^.*$";

        // Test different flag combinations create separate cache entries
        Pattern pattern1 = PatternCache.getPattern(regex, Pattern.UNIX_LINES);
        Pattern pattern2 = PatternCache.getPattern(regex, Pattern.DOTALL);
        Pattern pattern3 = PatternCache.getPattern(regex, 0); // no flags

        assertEquals(3, PatternCache.getCacheSize());
        assertNotSame("Different flags should create different patterns", pattern1, pattern2);
        assertNotSame("Different flags should create different patterns", pattern1, pattern3);
        assertNotSame("Different flags should create different patterns", pattern2, pattern3);

        // Same regex with same flags should return cached pattern
        Pattern pattern4 = PatternCache.getPattern(regex, Pattern.UNIX_LINES);
        assertEquals(3, PatternCache.getCacheSize());
        assertSame("Same regex with same flags should return cached pattern", pattern1, pattern4);
    }

    @Test
    public void testPatternFunctionality() {
        String regex = "\\d{4}-\\d{2}-\\d{2}";
        Pattern pattern = PatternCache.getPattern(regex);

        assertTrue("Pattern should match valid date", pattern.matcher("2023-12-25").matches());
        assertFalse("Pattern should not match invalid date", pattern.matcher("invalid-date").matches());
    }

    @Test
    public void testPatternFlagsWork() {
        String multilineText = "line1\nline2";

        // Test DOTALL flag
        Pattern dotallPattern = PatternCache.getPattern(".*", Pattern.DOTALL);
        assertTrue("DOTALL should match across newlines", dotallPattern.matcher(multilineText).matches());

        // Test without DOTALL flag
        Pattern normalPattern = PatternCache.getPattern(".*", 0);
        assertFalse("Without DOTALL should not match across newlines", normalPattern.matcher(multilineText).matches());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullRegex() {
        PatternCache.getPattern(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullRegexWithFlags() {
        PatternCache.getPattern(null, Pattern.UNIX_LINES);
    }

    @Test
    public void testNullAndEmptyInputs() {
        // Clear cache first to ensure clean state
        PatternCache.clearCache();

        // Empty string is a VALID regex pattern, should return Pattern object
        Pattern emptyPattern = PatternCache.getPattern("");
        assertNotNull("Empty regex should return valid Pattern", emptyPattern);
        assertTrue("Empty pattern should match empty string", emptyPattern.matcher("").matches());
        assertFalse("Empty pattern should not match non-empty string", emptyPattern.matcher("abc").matches());

        Pattern emptyPatternWithFlags = PatternCache.getPattern("", Pattern.DOTALL); // Use different flag!
        assertNotNull("Empty regex with flags should return valid Pattern", emptyPatternWithFlags);
        assertTrue("Empty pattern with flags should match empty string", emptyPatternWithFlags.matcher("").matches());

        // Should have cached 2 different entries (different flags)
        assertEquals("Should have cached empty pattern entries with different flags", 2, PatternCache.getCacheSize());
    }

    @Test(expected = PatternSyntaxException.class)
    public void testInvalidRegex() {
        // Invalid regex should throw PatternSyntaxException
        PatternCache.getPattern("[invalid");
    }

    @Test(expected = PatternSyntaxException.class)
    public void testInvalidRegexWithFlags() {
        // Invalid regex with flags should throw PatternSyntaxException
        PatternCache.getPattern("[invalid", Pattern.UNIX_LINES);
    }

    @Test
    public void testCacheClear() {
        // Add some patterns to cache
        PatternCache.getPattern("test1");
        PatternCache.getPattern("test2");
        PatternCache.getPattern("test3", Pattern.UNIX_LINES);

        assertEquals(3, PatternCache.getCacheSize());

        PatternCache.clearCache();
        assertEquals(0, PatternCache.getCacheSize());

        // After clearing, should recompile patterns
        Pattern pattern = PatternCache.getPattern("test1");
        assertEquals(1, PatternCache.getCacheSize());
    }

    @Test
    public void testDifferentRegexPatterns() {
        String[] regexes = {
                "^.*$",
                "\\d+",
                "[a-zA-Z]+",
                "test-\\w+-\\d+",
                "(?i)case-insensitive"
        };

        // Cache different patterns
        for (String regex : regexes) {
            PatternCache.getPattern(regex);
        }

        assertEquals(regexes.length, PatternCache.getCacheSize());

        // Verify each pattern works correctly
        Pattern digitPattern = PatternCache.getPattern("\\d+");
        assertTrue("Digit pattern should match numbers", digitPattern.matcher("12345").matches());
        assertFalse("Digit pattern should not match letters", digitPattern.matcher("abcde").matches());
    }

    @Test
    public void testPatternReusabilityInRealScenario() {
        // Simulate real Singer usage patterns
        String messageStartRegex = "^.*$";
        String filterRegex = "ERROR.*";

        // Simulate TextLogFileReaderFactory behavior
        Pattern messagePattern1 = PatternCache.getPattern(messageStartRegex, Pattern.UNIX_LINES);
        Pattern filterPattern1 = PatternCache.getPattern(filterRegex, Pattern.DOTALL);

        // Simulate another TextLogFileReader creation with same config
        Pattern messagePattern2 = PatternCache.getPattern(messageStartRegex, Pattern.UNIX_LINES);
        Pattern filterPattern2 = PatternCache.getPattern(filterRegex, Pattern.DOTALL);

        // Should reuse cached patterns
        assertSame("Message patterns should be cached", messagePattern1, messagePattern2);
        assertSame("Filter patterns should be cached", filterPattern1, filterPattern2);
        assertEquals("Should have 2 cached patterns", 2, PatternCache.getCacheSize());
    }
}
