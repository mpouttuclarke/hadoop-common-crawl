package com.amazon.hackarizona2017.hadoop;

import com.google.common.hash.BloomFilter;
import com.google.re2j.Matcher;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

/**
 * Generates sum and normalized terms.
 */
class SumSaturationMapper extends Mapper<Text, ArchiveReader, Text, TermBitmapCountWritable> {

    private static final Logger LOG = Logger.getLogger(JobTitleRankTool.class);

    public static final String INVALID_TERM = "INVALID";

    protected enum MapperCounter {
        RECORDS_IN,
        EMPTY_PAGE_TEXT,
        EXCEPTIONS,
        NON_PLAIN_TEXT,
        NO_MATCH
    }

    private final Text outKey = new Text();
    private final TermBitmapCountWritable outVal = new TermBitmapCountWritable();
    private final TermPatterns termPatterns = new TermPatterns();
    private Matcher matcher;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        matcher = termPatterns.initFromReader(new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream("/titles-IT.txt")))).matcher("");
    }

    @Override
    public void map(Text key, ArchiveReader value, Context context) throws IOException {
        for (ArchiveRecord r : value) {
            try {
                if (r.getHeader().getMimetype().equals("text/plain")) {
                    int sum = 0;
                    BloomFilter<String> bloom = termPatterns.newBloomForTerms();
                    // We need this counter because Map Reduce only sees one input record (normally one per line)
                    context.getCounter(MapperCounter.RECORDS_IN).increment(1);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(r.getHeader().getUrl() + " -- " + r.available());
                    }
                    outKey.set(new URL(r.getHeader().getUrl()).getHost());
                    // Convenience function that reads the full message into a raw byte array
                    byte[] rawData = IOUtils.toByteArray(r, r.available());
                    // Need alternate encoding for internationalization
                    String content = new String(rawData);
                    // Regex match the document
                    if (StringUtils.isEmpty(content)) {
                        context.getCounter(MapperCounter.EMPTY_PAGE_TEXT).increment(1);
                    } else {
                        matcher.reset(content);
                        while(matcher.find()) {
                            boolean termFound = false;
                            // Get the term group that matched (if any)
                            for (int group = 1; group < matcher.groupCount(); group++) {
                                if(matcher.group(group) != null) {
                                    bloom.put(termPatterns.getTermById(group));
                                    termFound = true;
                                    sum++;
                                    break;
                                }
                            }
                            if(!termFound) {
                                context.getCounter(MapperCounter.NO_MATCH).increment(1);
                            }
                        }
                    }
                    outVal.count.set(sum);
                    final byte[] bytes = termPatterns.getBytesForBloom(bloom);
                    outVal.bitmap.set(bytes, 0, bytes.length);
                    context.write(outKey, outVal);
                } else {
                    context.getCounter(MapperCounter.NON_PLAIN_TEXT).increment(1);
                }
            } catch (Exception ex) {
                LOG.error("Caught Exception", ex);
                context.getCounter(MapperCounter.EXCEPTIONS).increment(1);
            }
        }
    }

}
