package com.amazon.mattpc.hadoopexample;

import com.google.common.hash.BloomFilter;
import com.google.re2j.Pattern;
import org.apache.commons.lang.StringUtils;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Creates a matching pattern for terms and also maps a term to a termId and estimates bloom filter matches.
 */
public class TermUtils {

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

    /**
     * Gets a term using an id starting with 1.
     * @param termId
     * @return
     */
    public String getTermById(final int termId) {
        return terms.get(termId - 1);
    }

    /**
     * Extracts the bytes in the roaring bitmap.
     * @param roar
     * @return
     * @throws IOException
     */
    public static byte[] getBytes(MutableRoaringBitmap roar) throws IOException {
        final ByteArrayOutputStream bao = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(bao);
        roar.serialize(out);
        out.close();
        return bao.toByteArray();
    }
}
