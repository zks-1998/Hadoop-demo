package com.gzhu.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReduce extends Reducer<Text, FlowBean,Text, FlowBean> {
    FlowBean flowBean = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        long down = 0;
        long up = 0;

        for (FlowBean value : values) {
            down += value.getDownFlow();
            up += value.getUpFlow();
        }

        flowBean.setDownFlow(down);
        flowBean.setUpFlow(up);
        flowBean.setSumFlow();

        context.write(key,flowBean);
    }
}
