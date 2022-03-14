package com.gzhu.mapreduce.worldcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WorldCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable intWritable = new IntWritable();
    @Override
    // key代表每一个key，例如Kun，values是一个集合，里面存的是key对应的V值
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;

        // 将每一个Key对应的value汇总
        for (IntWritable value : values) {
            sum += value.get();
        }
        intWritable.set(sum);

        context.write(key,intWritable);
    }
}
