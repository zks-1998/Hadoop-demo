package com.gzhu.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class YarnTool implements Tool {
    private Configuration conf;
    @Override
    public int run(String[] args) throws Exception {
        // 1.获取job
        Job job = Job.getInstance(conf);

        // 2.设置jar路径
        job.setJarByClass(YarnDriver.class);

        // 3.关联mapper和reducer
        job.setMapperClass(YarnTool.WorldCountMapper.class);
        job.setReducerClass(YarnTool.WorldCountReduce.class);

        // 4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    // mapper
    public static class WorldCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        private Text text = new Text();
        private IntWritable intWritable = new IntWritable(1);
        @Override                  // 这里的value是每一行数据
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            // 1.根据原始数据获取一行，转换成字符串，例如 kun kun kun
            String string = value.toString();

            // 2.切割每一行单词  [kun,kun,kun]
            String[] words = string.split(" ");

            // 3.循环写出，每一个kun都为1 K-V kun-1，Mapper阶段不汇总，所以每一个都是1
            for (String word : words) {
                // 将String类型转换成Text类型
                text.set(word);
                // write里面的参数为输出的两个参数类型 Text, IntWritable
                // 这里输出三个kun-1
                context.write(text,intWritable);
            }
        }
    }
    // reduce
    public static class WorldCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
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
}
