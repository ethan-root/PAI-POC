package com.example.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * InfiniteFlinkJobB: Emits a message every 2 seconds.
 * Used for testing long-running jobs.
 */
public class InfiniteFlinkJobB {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stream = env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning) {
                    ctx.collect("Job B: " + System.currentTimeMillis());
                    Thread.sleep(2000); // 2 seconds
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        stream.print();

        env.execute("POC-Infinite-Job-B");
    }
}
