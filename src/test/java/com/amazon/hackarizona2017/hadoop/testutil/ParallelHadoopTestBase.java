package com.amazon.hackarizona2017.hadoop.testutil;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import java.io.File;

/**
 *  Allows parallel execution of tests without clobbering temp files created by Hadoop.
 */
public abstract class ParallelHadoopTestBase extends Configured {

	public ParallelHadoopTestBase() {
        super(new Configuration(true));
        File file = new File(getOutputPath());
        FileUtils.deleteQuietly(file);
        String uriString = file.toURI().toString();
        getConf().set("fs.defaultFS", uriString);
        getConf().set("hadoop.tmp.dir", uriString + "/.tmp");
    }

    public String getOutputPath() {
        return "target/testutil/" + getClass().getName();
    }
}
