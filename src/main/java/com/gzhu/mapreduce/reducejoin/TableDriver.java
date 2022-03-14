package com.gzhu.mapreduce.reducejoin;

import com.gzhu.mapreduce.partition.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TableDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置jar路径
        job.setJarByClass(TableDriver.class);

        // 3.关联mapper和reducer
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReduce.class);

        // 4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        // 5.设置最终输出的kv类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);


        // 6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("F:\\input\\inputtable"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\output\\outputtable"));

        // 7.提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
