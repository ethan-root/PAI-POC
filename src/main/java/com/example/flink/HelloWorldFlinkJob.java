package com.example.flink;  // TODO: 修改为你的包名

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * POC: 最简单的 Flink Hello World 测试作业
 * 
 * 目的：验证 GitHub Action → OSS → 阿里云 Flink 部署流程
 */
public class HelloWorldFlinkJob {

    public static void main(String[] args) throws Exception {
        // 打印启动信息
        System.out.println("========================================");
        System.out.println("  POC: Hello World Flink Job Starting");
        System.out.println("  Deployed via GitHub Action!");
        System.out.println("  Timestamp: " + System.currentTimeMillis());
        System.out.println("========================================");

        // 获取 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建简单的数据流
        DataStream<String> stream = env.fromElements(
            "Hello Flink!",
            "Deployed via GitHub Action",
            "Running on Aliyun Flink Serverless"
        );

        // 处理并打印
        stream
            .map(msg -> "[POC OUTPUT] " + msg.toUpperCase())
            .print();

        // 执行作业
        env.execute("POC-HelloWorld-Job");

        System.out.println("========================================");
        System.out.println("  POC: Hello World Flink Job Completed!");
        System.out.println("========================================");
    }
}
