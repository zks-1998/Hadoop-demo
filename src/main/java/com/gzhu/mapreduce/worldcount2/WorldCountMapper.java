package com.gzhu.mapreduce.worldcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
泛型一:KEYIN：LongWritable，对应的Mapper的输入key。输入key是每行的行首偏移量
泛型二: VALUEIN：Text，对应的Mapper的输入Value。输入value是每行的内容
泛型三:KEYOUT：对应的Mapper的输出key，根据业务来定义
泛型四:VALUEOUT：对应的Mapper的输出value，根据业务来定义

KEYIN和VALUEIN写死(LongWritable,Text)。KEYOUT和VALUEOUT不固定,根据业务来定

Writable机制是Hadoop自身的序列化机制，常用的类型：
	a. LongWritable
	b. Text(String)  Text对应java中String类型
	c. IntWritable
	d. NullWritable
*/

/*
* 这里的输入数据为       输出数据 kun 3 song 2 zhang liu 1 wang 1 li 2
*  kun kun kun
*  song song
*  zhang
*  liu
*  wang
*  li li
*/
public class WorldCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
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
