package com.gzhu.mapreduce.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
// 1.实现Writable接口
public class FlowBean implements Writable {
    private long upFlow; //上行流量
    private long downFlow; //下行流量
    private long sumFlow; //总流量

    // 2.反序列化，需要反射调用空参构造函数，所以必须有空参函数
    public FlowBean(){
    }
    // 3.重写序列化方法，注意！！！反序列化顺序要和序列化顺序完全一致！！！
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }
    // 4.反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }
    // 5.get set方法
    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = upFlow + downFlow;
    }
    // 6.toString()方法
    @Override
    public String toString() {
        return
        "upFlow=" + upFlow +
        ", downFlow=" + downFlow +
        ", sumFlow=" + sumFlow ;
    }
    /*
    * 7.Map<K1,V,K2,V>
    如果将自定义的bean放在K2传输，必须要实现Comparable接口，K2必须能排序
    */}
