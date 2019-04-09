package com.mx.mr.sort;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {

    private static final Log LOG = LogFactory.getLog(SortMapper.class);
    private SortBean bean = new SortBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.info("mapper is called...");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] str = StringUtils.split(value.toString(), "\t");
        if (str.length != 4) {
            return;
        }
        bean.clear();
        bean.setId(str[0]);
        bean.setName(str[1]);
        bean.setSex(str[2]);
        bean.setGrade(str[3]);
        context.write(bean, NullWritable.get());
    }
}
