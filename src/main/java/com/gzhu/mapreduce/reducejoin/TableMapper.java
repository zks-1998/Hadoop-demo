package com.gzhu.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {
    String fileName;
    Text text = new Text();
    TableBean tableBean = new TableBean();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        // 提前获取分片的文件名字
        FileSplit split = (FileSplit) context.getInputSplit();

        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        String string = value.toString(); // 获取一行数据

        String[] split = string.split("\t");

        // 判断属于哪个文件
        if(fileName.equals("order.txt")){
            text.set(split[1]); // Map输出key

            tableBean.setId(split[0]);
            tableBean.setPid(split[1]);
            tableBean.setAmount(Integer.parseInt(split[2]));
            tableBean.setPname("");
            tableBean.setFlag("order");
        }else{
            text.set(split[0]); // Map输出key

            tableBean.setId("");
            tableBean.setPid(split[0]);
            tableBean.setAmount(0);
            tableBean.setPname(split[1]);
            tableBean.setFlag("pd");
        }

        context.write(text,tableBean);
    }
}
