package com.mx.mr.sort;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<SortBean, NullWritable, SortBean, NullWritable> {

    private static final Log LOG = LogFactory.getLog(SortReducer.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.info("reduce is called...");
    }

    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
