package com.amazon.mattpc.hadoopexample;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.archive.io.ArchiveReader;

import java.io.IOException;

/**
 * Read a compressed WARC as a single input split.
 */
public class WARCInputFormat extends FileInputFormat<Text, ArchiveReader> {

    @Override
    public RecordReader<Text, ArchiveReader> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new WARCRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        /* Files are compressed */
        return false;
    }
}
