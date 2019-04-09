package com.mx.mr.add;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class AddMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text text = new Text();
    private Log log = LogFactory.getLog(AddMapper.class);

    private static final String INPUT_PATH = "/data/collecttmp";
    
    private static final String OUTPUT_PATH_BASE = "/data/collect";

    private MultipleOutputs<Text, NullWritable> mos;
    private String fileName;
    
    private String output;
    
    private StringBuffer sb = new StringBuffer();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        log.info("map is called...");
        mos = new MultipleOutputs(context);

        FileSplit fs = (FileSplit) context.getInputSplit();
        
        String pFileName = fs.getPath().getParent().getName();
        log.info("解析父文件路径---->" + pFileName);
        
        fileName = fs.getPath().getName().split("\\.")[0];
        log.info("解析文件名---->" + fileName);
        
        output = OUTPUT_PATH_BASE + "/" + pFileName + "/" + "part-" + System.currentTimeMillis();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        sb.setLength(0);
        sb.append(line).append("\t").append(fileName);
        text.clear();
        text.set(sb.toString());
        mos.write(text, NullWritable.get(), output);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	mos.close();
    }
    
}
