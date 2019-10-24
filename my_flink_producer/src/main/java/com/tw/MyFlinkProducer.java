package com.tw;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class MyFlinkProducer {

    public void runJob() {


        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<WikipediaEditEvent> inputStream = see.addSource(new WikipediaEditsSource());


        KeyedStream<WikipediaEditEvent, String> keyedStream = inputStream
                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
                    @Override
                    public String getKey(WikipediaEditEvent event) {
                        return event.getUser();
                    }
                });


        DataStream<Tuple2<String, Long>> result = keyedStream
                .timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return new Tuple2<>("", 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(WikipediaEditEvent value, Tuple2<String, Long> accumulator) {
                        accumulator.f0 = value.getUser();
                        accumulator.f1 += value.getByteDiff();
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                        return new Tuple2<>(a.f0, a.f1 + b.f1);
                    }
                });


        result.map(new MapFunction<Tuple2<String, Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> tuple) {
                return tuple.toString();
            }
        })
                .addSink(new FlinkKafkaProducer<>("10.207.20.217:9092", "wiki-results", new SimpleStringSchema()));

        try {
            see.execute();
        }catch (Exception e){

        }
    }
}
