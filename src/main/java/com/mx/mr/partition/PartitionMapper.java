package com.mx.mr.partition;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartitionMapper extends Mapper<LongWritable, Text, PartitionBean, NullWritable> {

    private static final Log LOG = LogFactory.getLog(PartitionMapper.class);

    PartitionBean bean = new PartitionBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.info("mapper is called...");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] arr = line.split("\t");
        if (arr.length == 4) {
            bean.clear();
            bean.setId(arr[0]);
            bean.setName(arr[1]);
            bean.setSex(arr[2]);
            bean.setGrade(arr[3]);
            context.write(bean, NullWritable.get());
        }

    }


}
