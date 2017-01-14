package com.amazon.hackarizona2017.hadoop;

import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class TermBitmapCountWritableTest {
    @Test
    public void testSerialize() {
        TermBitmapCountWritable w0 = new TermBitmapCountWritable();
        w0.bitmap.set("bob".getBytes(), 0, 3);
        w0.count.set(5);
        try {
            final ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream(1024);
            final DataOutputStream dataOutput = new DataOutputStream(arrayOutputStream);
            w0.write(dataOutput);
            dataOutput.close();
            TermBitmapCountWritable w1 = new TermBitmapCountWritable();
            w1.readFields(new DataInputStream(new ByteArrayInputStream(arrayOutputStream.toByteArray())));
            assertEquals(w0, w1);
            assertEquals(w0.hashCode(), w1.hashCode());
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }
}
