package com.mx.mr.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PartitionReducer extends Reducer<PartitionBean, NullWritable, PartitionBean, NullWritable> {


    @Override
    protected void reduce(PartitionBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key, NullWritable.get());
    }
}
