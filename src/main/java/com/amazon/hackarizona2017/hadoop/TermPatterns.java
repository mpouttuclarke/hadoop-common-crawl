package com.amazon.hackarizona2017.hadoop;

import com.google.common.hash.BloomFilter;
import com.google.re2j.Pattern;
import javafx.scene.effect.Bloom;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Creates a matching pattern for terms and also maps a term to a termId and estimates bloom filter matches.
 */
public class TermPatterns {

    public static final TermFunnel TERM_FUNNEL = new TermFunnel();
    private final List<String> terms = new ArrayList<>(1024);

    public Pattern initFromReader(BufferedReader reader) throws IOException {
        String line;
        StringBuilder builder = new StringBuilder();
        while((line = reader.readLine()) != null) {
            terms.add(line);
            if(builder.length() > 0) {
                builder.append("|");
            }
            builder.append("\\W*(");
            builder.append(StringUtils.trim(line).replaceAll("\\W+", "\\\\W+"));
            builder.append(")\\W*");
        }
        return Pattern.compile(builder.toString(), Pattern.CASE_INSENSITIVE);
    }

    public BloomFilter<String> newBloomForTerms() {
        return BloomFilter.create(TERM_FUNNEL, terms.size(), 0.001);
    }

    /**
     * Gets a term using an id starting with 1.
     * @param termId
     * @return
     */
    public String getTermById(final int termId) {
        return terms.get(termId - 1);
    }

    /**
     * Estimates the count of term matches.
     * @param bloom
     * @return
     */
    public int estimateMatchCount(BloomFilter<String> bloom) {
        int total = 0;
        for(String term : terms) {
            if (bloom.mightContain(term)) {
                total++;
            }
        }
        return total;
    }

    /**
     * Gets the raw bytes for a bloom filter.
     * @param bloom
     * @return
     * @throws IOException
     */
    public byte[] getBytesForBloom(BloomFilter<String> bloom) throws IOException {
        final ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream(1024);
        bloom.writeTo(arrayOutputStream);
        arrayOutputStream.close();
        return arrayOutputStream.toByteArray();
    }
}
