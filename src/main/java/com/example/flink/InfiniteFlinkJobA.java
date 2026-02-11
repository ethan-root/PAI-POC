package com.example.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * InfiniteFlinkJobA: Emits a message every 1 second.
 * Used for testing long-running jobs.
 */
public class InfiniteFlinkJobA {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stream = env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning) {
                    ctx.collect("Job A: " + System.currentTimeMillis());
                    Thread.sleep(1000); // 1 second
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        stream.print();

        env.execute("POC-Infinite-Job-A");
    }
}
