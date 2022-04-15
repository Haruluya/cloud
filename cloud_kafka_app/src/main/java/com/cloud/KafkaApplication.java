package com.cloud;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * kafka stream的处理类。
 * kafka stream对log topic的数据处理，存放在 recommender topic。
 * @author haruluya
 */
public class KafkaApplication {
    public static void main(String[] args) {
        String brokers = "localhost:9092";
        String zookeepers = "localhost:2181";

        // 输入和输出的topic
        String from = "log";
        String to = "recommender";

        // kafka stream 配置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        StreamsConfig config = new StreamsConfig(settings);

        // 拓扑构建器
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", from)
                .addProcessor("PROCESSOR", ()->new KafkaLogProcess(), "SOURCE")
                .addSink("SINK", to, "PROCESSOR");

        // 创建kafka stream
        KafkaStreams streams = new KafkaStreams( builder, config );

        streams.start();
        System.out.println("kafka stream begin!");
    }
}
