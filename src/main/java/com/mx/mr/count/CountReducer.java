package com.mx.mr.count;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private static LongWritable lw = new LongWritable();
    private static final Log LOG = LogFactory.getLog(CountReducer.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.info("reduce is called...");
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long i = 0L;
        for (LongWritable l : values) {
            i += l.get();
        }
        lw.set(i);
        context.write(key, lw);
    }
}
