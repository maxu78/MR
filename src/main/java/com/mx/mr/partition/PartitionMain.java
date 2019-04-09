package com.mx.mr.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartitionMain {


    private static Configuration configuration;

    static {

        try {
            configuration = new Configuration();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {


        String input = "/data/input/";
        String output = "/data/output/";

        Job job = Job.getInstance(configuration, "partition");

        job.setJarByClass(PartitionMain.class);

        job.setMapperClass(PartitionMapper.class);
        job.setMapOutputKeyClass(PartitionBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(PartitionReducer.class);
        job.setOutputKeyClass(PartitionBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(Partition.class);
        job.setNumReduceTasks(10);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }
}
