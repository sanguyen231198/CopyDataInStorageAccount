package com.pluralsight.streaming;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.FileSystemBlobStore;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
public class Word {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);


        DataStream<String> text = env.readTextFile("wasbs://flink@nifiteststr01.blob.core.windows.net/input/");
//        env.addSource()

        DataStream<Tuple2<String, Integer>> dataStream = text
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .sum(1);


//          dataStream.print();
        dataStream.writeAsText("wasbs://target@nifiteststr01.blob.core.windows.net/output/output.txt");
//        dataStream.addSink

        env.execute("Text WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }


}
