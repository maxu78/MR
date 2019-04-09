package com.mx.mr.count;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class CountMain {

    private static final Log LOG = LogFactory.getLog(CountMain.class);

    private static Configuration configuration;


    static {

        try {
            configuration = new Configuration();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        LOG.info(configuration.get("fs.default.name"));

        Job job = Job.getInstance(configuration);

        job.setJarByClass(CountMain.class);


        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path in = new Path("/data");
        Path out = new Path("/output/" + System.currentTimeMillis());

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}
