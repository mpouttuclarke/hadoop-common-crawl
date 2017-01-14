package com.amazon.hackarizona2017.hadoop;

import com.google.common.hash.BloomFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Aggregates the sum and saturation per domain.
 */
public class SumSaturationReducer
        extends Reducer<Text, TermBitmapCountWritable, Text, TermBitmapCountWritable> {
    public static final String DELIM = "\t";
    private Text resultKey = new Text();
    private TermBitmapCountWritable resultValue = new TermBitmapCountWritable();
    private final TermPatterns termPatterns = new TermPatterns();

    @Override
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        super.setup(context);
        termPatterns.initFromReader(new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream("/titles-IT.txt"))));
    }

    public void reduce(Text key, Iterable<TermBitmapCountWritable> values,
                       Context context) throws IOException, InterruptedException {
        long sum = 0;
        BloomFilter<String> bloomAgg = null;
        for (TermBitmapCountWritable val : values) {
            sum += val.count.get();
            BloomFilter<String> bloomIn =
                    BloomFilter.readFrom(new ByteArrayInputStream(val.bitmap.getBytes()), TermPatterns.TERM_FUNNEL);
            if(bloomAgg == null) {
                bloomAgg = bloomIn;
            } else {
                bloomAgg.putAll(bloomIn);
            }
        }
        resultKey.set(key + DELIM + termPatterns.estimateMatchCount(bloomAgg));
        resultValue.count.set(sum);
        final byte[] bytesForBloom = termPatterns.getBytesForBloom(bloomAgg);
        resultValue.bitmap.set(bytesForBloom, 0, bytesForBloom.length);
        context.write(resultKey, resultValue);
    }
}
