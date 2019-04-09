package com.mx.mr.add;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AddMain {

    private static final Log LOG = LogFactory.getLog(AddMain.class);

    private static Configuration configuration;

    private static FileSystem fs;

    private static final String INPUT_PATH_TMP = "/data/collecttmp";

    private static final String INPUT_PATH = "/data/collect";

    private static final String INPUT_WORKING_PATH = INPUT_PATH + "/input";

    static {
        try {
            configuration = new Configuration();
            fs = FileSystem.get(configuration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        boolean b = before();
        if(b) {
            b = execute();
            if(b) {
                after();
            }
        } else {
            LOG.info("before--->" + b);
        }
    }

    public static boolean execute() throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(configuration, "Add");

        job.setJarByClass(AddMain.class);
        TextInputFormat.setMinInputSplitSize(job, 1024 * 1024 * 512L);
        TextInputFormat.setMaxInputSplitSize(job, 1024 * 1024 * 1024L);
        job.setMapperClass(AddMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPaths(job, INPUT_WORKING_PATH);
        FileOutputFormat.setOutputPath(job, new Path(INPUT_PATH + "/" + System.currentTimeMillis()));

        return job.waitForCompletion(true);
    }

    public static boolean before() throws IOException {

        boolean b = true;
        mkdir(INPUT_WORKING_PATH);
        try {
            //0. 先判断工作文件夹是否有文件，如果有则报错
            FileStatus[] fsArr = fs.listStatus(new Path(INPUT_WORKING_PATH));
            if (fsArr.length != 0) {
                throw new Exception("工作文件夹中有文件，请确认！");
            }
            //1. 清理处理文件夹
            delete(INPUT_WORKING_PATH);
            //2. 移动文件到处理文件夹
            move(INPUT_PATH_TMP, INPUT_WORKING_PATH);
        } catch (Exception e) {
            b = false;
            e.printStackTrace();
        }

        return b;
    }

    public static void after() throws IOException {

        //删除工作文件夹中的文件
        delete(INPUT_WORKING_PATH);
    }


    public static void delete(String src) throws IOException {
        Path p1 = new Path(src);
        boolean flag = fs.delete(p1,true);
        LOG.info("delete--->" + flag);
    }

    //移动
    //注意 此时des文件夹一定不能存在，否则rename会有问题!!!
    public static void move(String src, String des) throws Exception {
        Path p1 = new Path(src);
        Path p2 = new Path(des);
        if (!fs.exists(p2)) {
            boolean flag = fs.rename(p1, p2);
            if (flag) {
                //重建文件夹
                mkdir(src);
            }
        } else {
            throw new Exception(des + "已存在");
        }
    }

    //建立文件夹
    public static void mkdir(String path) throws IOException {
        Path p = new Path(path);
        boolean flag = fs.mkdirs(p);
        LOG.info("mkdir--->" + flag);
    }
}
