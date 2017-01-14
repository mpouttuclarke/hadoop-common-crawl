package com.amazon.hackarizona2017.hadoop;

import com.google.common.hash.BloomFilter;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import static org.junit.Assert.*;

/**
 *
 */
public class TermPatternsTest {
    @Test
    public void testHappyPath() {
        TermPatterns target = new TermPatterns();
        try {
            final String string = "Term One\n Term Two\nTerm\0 Three ";
            Pattern pattern = target.initFromReader(new BufferedReader(
                    new StringReader(string)));
            assertEquals("\\W*(Term\\W+One)\\W*|\\W*(Term\\W+Two)\\W*|\\W*(Term\\W+Three)\\W*",
                    pattern.toString());
            final Matcher matcher = pattern.matcher(string);
            assertTrue("find", matcher.find() && matcher.find() && matcher.find());
            assertEquals("group count", 3, matcher.groupCount());
            assertEquals("Term\0 Three", matcher.group(3));
            assertFalse(matcher.find());
            BloomFilter<String> bloom = BloomFilter.create(new TermFunnel(), 10, .0001);
            bloom.put(target.getTermById(2));
            bloom.put(target.getTermById(3));
            assertEquals("bloom match",2, target.estimateMatchCount(bloom));
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }
}
