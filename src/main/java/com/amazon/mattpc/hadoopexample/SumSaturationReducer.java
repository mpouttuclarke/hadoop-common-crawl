package com.amazon.mattpc.hadoopexample;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.*;

/**
 * Aggregates the sum and saturation per domain.
 */
public class SumSaturationReducer
        extends Reducer<Text, TermBitmapCountWritable, Text, TermBitmapCountWritable> {
    public static final String DELIM = "\t";
    private Text resultKey = new Text();
    private TermBitmapCountWritable resultValue = new TermBitmapCountWritable();
    private final TermUtils termPatterns = new TermUtils();

    @Override
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        super.setup(context);
        termPatterns.initFromReader(new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream("/titles-IT.txt"))));
    }

    public void reduce(Text key, Iterable<TermBitmapCountWritable> values,
                       Context context) throws IOException, InterruptedException {
        long sum = 0;
        MutableRoaringBitmap roarAgg = null;
        for (TermBitmapCountWritable val : values) {
            sum += val.count.get();
            final MutableRoaringBitmap roarIn = MutableRoaringBitmap.bitmapOf();
            roarIn.deserialize(new DataInputStream(new ByteArrayInputStream(val.bitmap.getBytes())));
            if(roarAgg == null) {
                roarAgg = roarIn;
            } else {
                roarAgg.or(roarIn);
            }
        }
        resultKey.set(key + DELIM + roarAgg.getCardinality());
        resultValue.count.set(sum);
        final byte[] bytesForBloom = TermUtils.getBytes(roarAgg);
        resultValue.bitmap.set(bytesForBloom, 0, bytesForBloom.length);
        context.write(resultKey, resultValue);
    }
}
