package com.gzhu.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class YarnDriver {
    private static Tool tool;
    public static void main(String[] args) throws Exception {
        // 创建配置信息
        Configuration configuration = new Configuration();

        switch (args[0]){
            case "wordcount":
                tool = new YarnTool();
                break;
            default:
                throw new RuntimeException("no no no");
        }

        int run = ToolRunner.run(configuration,tool, Arrays.copyOfRange(args,1,args.length));

        System.exit(run);
    }
}
