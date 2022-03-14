package com.gzhu.put;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class JDBCMapper extends Mapper<LongWritable,MyDBWritable,LongWritable, LongWritable> {
    private LongWritable longWritable = new LongWritable();
    private LongWritable outWritable = new LongWritable(1);
    @Override
    protected void map(LongWritable key, MyDBWritable value, Mapper<LongWritable, MyDBWritable, LongWritable, LongWritable>.Context context) throws IOException, InterruptedException {
        Long age = value.getAge(); // 读取每一行的年龄作为K

        longWritable.set(age);

        context.write(longWritable, outWritable);
    }
}
