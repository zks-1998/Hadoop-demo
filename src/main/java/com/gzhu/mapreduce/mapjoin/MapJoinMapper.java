package com.gzhu.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private HashMap<String, String> map = new HashMap<>();
    private Text text = new Text();
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 获取缓存文件，并把文件内容封装到集合
        URI[] files = context.getCacheFiles();

        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(files[0]));

        // 从流中读数据
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        // 读取的是一行
        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){
            // 切割
            String[] split = line.split("\t");

            map.put(split[0],split[1]);
        }

        // 关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        //通过大表每行数据的pid,去map里面取出pname
        String pname = map.get(fields[1]);

        //将大表每行数据的pid替换为pname
        text.set(fields[0] + "\t" + pname + "\t" + fields[2]);

        //写出
        context.write(text,NullWritable.get());
    }
}
