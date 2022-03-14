package com.gzhu.put;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JDBCReduce extends Reducer<LongWritable, LongWritable,MyDBWritable, NullWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Reducer<LongWritable, LongWritable, MyDBWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        long count = 0L;

        for (LongWritable value : values) {  // 比如K : 21  则此时values是3个1的集合，所以要循环累加
            count += value.get();
        }

        MyDBWritable myDBWritable = new MyDBWritable();
        myDBWritable.setAg(key.get());
        myDBWritable.setCount(count);

        context.write(myDBWritable,NullWritable.get());
    }
}
