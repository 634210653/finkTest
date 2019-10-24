package com.tw;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class MyFlinkConsumer {


    public  void runJob(){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.207.20.217:9092");
        properties.setProperty("group.id", "test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.addSource( new FlinkKafkaConsumer<String>("wiki-results", new SimpleStringSchema(),properties));

        inputStream.print();

        try {
            env.execute();
        }catch (Exception e){

        }
    }
}
