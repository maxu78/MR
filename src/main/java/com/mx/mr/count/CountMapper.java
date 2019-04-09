package com.mx.mr.count;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final Log LOG = LogFactory.getLog(CountMain.class);

    private static Text text = new Text();
    private LongWritable lw = new LongWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.info("mapper is called...");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strArr = StringUtils.split(line, "\t");
        if (strArr.length == 4) {
            //统计成绩数量
            String grade = strArr[3];
            text.clear();
            text.set(grade);
            context.write(text, lw);
        }
    }
}
