package com.amazon.hackarizona2017.hadoop;

import com.amazon.hackarizona2017.hadoop.testutil.ParallelHadoopTestBase;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

/**
 *
 */
public class JobTitleRankToolTest extends ParallelHadoopTestBase {

    @Test
    public void test() {
        try {
            assertEquals(0,
                    ToolRunner.run(getConf(), new JobTitleRankTool(),
                            new String[] {
                                    "src/test/resources/data/wet",
                                    getOutputPath() }));
            List<String> lines = FileUtils
                    .readLines(new File(getOutputPath() + "/part-r-00000"));
            assertEquals(1L, lines.size());
            String line = lines.get(0);
            assertTrue(line.startsWith("news.bbc.co.uk\t2\t8"));
        } catch (Exception e) {
            e.printStackTrace();
            fail(String.valueOf(e));
        }
    }

}
