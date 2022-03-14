package com.gzhu.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
// Reduce阶段输出的K-V
public class WebRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream web1;
    private FSDataOutputStream web2;

    public WebRecordWriter(TaskAttemptContext job) {
        // 创建两条流
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());

            web1 = fs.create(new Path("F:\\output\\outputformat\\web1"));
            web2 = fs.create(new Path("F:\\output\\outputformat\\web2"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // 具体写
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String str = text.toString();
        String []arr = str.split("\\.");
        if(arr[1].length() <= 4){
            web1.writeBytes(str + "\n");
        }else{
            web2.writeBytes(str + "\n");
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(web1);
        IOUtils.closeStream(web2);
    }
}
