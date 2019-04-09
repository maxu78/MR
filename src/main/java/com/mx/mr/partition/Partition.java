package com.mx.mr.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partition extends Partitioner<PartitionBean, NullWritable>{


    @Override
    public int getPartition(PartitionBean partitionBean, NullWritable nullWritable, int numPartitions) {

        int p = 0;
        String grade = partitionBean.getGrade();
        int hg1 = grade == null ? 0 : grade.hashCode();

        p = hg1 % 3;

        return p;
    }
}
