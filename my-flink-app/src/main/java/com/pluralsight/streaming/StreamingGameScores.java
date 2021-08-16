package com.pluralsight.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

public class StreamingGameScores {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        if(!parameterTool.has("host" ) || !parameterTool.has("port")){
            System.out.println("Please specify values for --host and --port");
            System.exit(1);
            return;
        }

        DataStream<String> dataStream = env.socketTextStream(parameterTool.get("host"), Integer.parseInt(parameterTool.get("port")));
        DataStream<Tuple2<String, Integer>> gameScores = dataStream
                .map(new ExtractPlayerAndScoreFn())
                .filter(new FilterPlayerAboveThreshold(100));
//        DataStream<String> gameScores = dataStream
//                .map(new ExtractPlayerAndScoreFn())
//                .filter(new FilterPlayerAboveThreshold(100))
//                .map(new ConvertToStringFn());

//        final StreamingFileSink<String> sink = StreamingFileSink
//                .<String>forRowFormat(new Path("src/main/resources/streamingSink"),
//                        new SimpleStringEncoder<>("UTF-8"))
//                .build();
//        gameScores.addSink(sink);
        gameScores.print();

        env.execute("Streaming Game Scores");

    }

    private static class ExtractPlayerAndScoreFn implements MapFunction<String, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            String[] tokens = s.trim().split(",");
            return Tuple2.of(tokens[0].trim(), Integer.parseInt(tokens[1].trim()));
        }
    }

    private static class FilterPlayerAboveThreshold implements FilterFunction<Tuple2<String, Integer>> {
        private int scoreThreshold = 0;

        public FilterPlayerAboveThreshold(int scoreThreshold) {
            this.scoreThreshold = scoreThreshold;
        }

        @Override
        public boolean filter(Tuple2<String, Integer> playerScore) throws Exception {
            return playerScore.f1 > scoreThreshold;
        }
    }

    private static class ConvertToStringFn implements MapFunction<Tuple2<String, Integer>, String> {

        @Override
        public String map(Tuple2<String, Integer> playerScores) throws Exception {
            return playerScores.f0 + ", " + playerScores.f1;
        }
    }
}
