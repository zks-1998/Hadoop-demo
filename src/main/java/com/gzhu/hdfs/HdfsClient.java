package com.gzhu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {
    FileSystem fs;
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接集群的nn地址，8020是内部通信端口而非web端口
        final URI uri = new URI("hdfs://hadoop102:8020");
        //创建配置文件
        Configuration configuration = new Configuration();
        //用户
        String user = "gzhu";
        // 获取客户端对象
        fs = FileSystem.get(uri,configuration,user);
    }
    @After
    public void close() throws IOException {
        // 关闭资源
        fs.close();
    }
    // 创建文件夹
    @Test
    public void testMkdir() throws IOException, URISyntaxException, InterruptedException {
        fs.mkdirs(new Path("/gzhu"));
    }

    // 上传文件
    @Test
    public void testPut() throws IOException, URISyntaxException, InterruptedException {
        // 参数一：是否删除源文件  参数二：是否覆盖 参数三：本地文件路径 参数四：HDFS文件路径
        fs.copyFromLocalFile(false,true,new Path("F:\\hadoop_test\\computer.txt"),new Path("/gzhu"));
    }
    // 文件下载
    @Test
    public void testGet() throws IOException {
        // 参数一：是否删除源文件 参数二：HDFS路径 参数三：本地路径 参数四：是否循环校验
        fs.copyToLocalFile(false,new Path("/gzhu"),new Path("F:\\"),true);
    }
    // 文件删除
    @Test
    public void testRm() throws IOException {
        // 参数一：要删除的路径 参数二：是否递归删除
        fs.delete(new Path("/gzhu/computer.txt"),false);
    }
    // 文件目录名称修改
    @Test
    public void testMv() throws IOException {
        fs.rename(new Path("/gzhu/开发基本.md"),new Path("/gzhu/程序开发.md"));
    }
    // 文件剪切移动
    @Test
    public void testGo() throws IOException {
        fs.rename(new Path("/gzhu/程序开发.md"),new Path("/java"));
    }
    // 获取文件详情信息
    @Test
    public void fileDetail() throws IOException {
        // 参数一：路径 参数二：是否递归
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

        // 遍历文件
        while(files.hasNext()){
            LocatedFileStatus fileStatus = files.next();

            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    // 判断是文件还是目录
    @Test
    public void testfile() throws IOException {
        FileStatus[] listFiles = fs.listStatus(new Path("/"));

        for (FileStatus file : listFiles
             ) {
            if(file.isFile()){
                System.out.println("文件："+file.getPath().getName());
            }else{
                System.out.println("目录："+file.getPath().getName());
            }
        }
    }
}
