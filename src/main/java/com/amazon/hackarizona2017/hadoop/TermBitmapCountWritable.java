package com.amazon.hackarizona2017.hadoop;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Keeps a count and a bitmap of encoded terms.
 */
public class TermBitmapCountWritable implements Writable {
    public final LongWritable count = new LongWritable();
    public final BytesWritable bitmap = new BytesWritable();

    public TermBitmapCountWritable() {}

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        count.write(dataOutput);
        bitmap.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count.readFields(dataInput);
        bitmap.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TermBitmapCountWritable that = (TermBitmapCountWritable) o;

        if (count != null ? !count.equals(that.count) : that.count != null) return false;
        return bitmap != null ? bitmap.equals(that.bitmap) : that.bitmap == null;
    }

    @Override
    public int hashCode() {
        int result = count != null ? count.hashCode() : 0;
        result = 31 * result + (bitmap != null ? bitmap.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return count + "\t" + Base64.encodeBase64URLSafeString(bitmap.getBytes());
    }
}
