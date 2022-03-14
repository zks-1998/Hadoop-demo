package com.gzhu.mapreduce.sort.allsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,FlowBean, Text> {
    Text text = new Text(); // V
    FlowBean flowBean = new FlowBean(); // K
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        String string = value.toString();

        String []words = string.split("\t");

        String keyNumber = words[1];
        text.set(keyNumber);

        flowBean.setDownFlow(Long.parseLong(words[words.length - 3]));
        flowBean.setUpFlow(Long.parseLong(words[words.length - 2]));
        flowBean.setSumFlow();

        context.write(flowBean,text);
    }
}
