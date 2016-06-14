package com.gmail.samuel.kerrien.demo_1;

import com.gmail.samuel.kerrien.KafkaConfig;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SimpleConsumer {

    private final ConsumerConnector consumer;

    public SimpleConsumer() {
        final Properties properties = buildProperties();
        ConsumerConfig config = new ConsumerConfig(properties);
        consumer = Consumer.createJavaConsumerConnector(config);
    }

    private Properties buildProperties() {
        final Properties properties = new Properties();
        properties.put("zookeeper.connect", KafkaConfig.ZOOKEEPER);
        properties.put("group.id", "demo1-group");
        properties.put("zookeeper.session.timeout.ms", "500");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("auto.commit.interval.ms", "1000");
        return properties;
    }

    public void run(String topic) {
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topic, 1);

        int i = 0;
        final long start = System.currentTimeMillis();
        Map<String, List<KafkaStream<byte[], byte[]>>> streamsByTopic = consumer.createMessageStreams(topicMap);
        List<KafkaStream<byte[], byte[]>> streams = streamsByTopic.get(topic);
        for (KafkaStream<byte[], byte[]> stream : streams) {
            ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
            while (iterator.hasNext()) {
                i++;
                MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();
//                System.out.println( new String( messageAndMetadata.message() ) );
                if ((i % 5000) == 0) {
                    float throughput = ((float) i) / ((System.currentTimeMillis() - start) / 1000);
                    System.out.println(String.format("Read %,d [throughput: %,.2f msg/sec]", i, throughput));
                }
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: SimpleConsumer <topic>");
            System.exit(1);
        }

        final String topic = args[0];

        SimpleConsumer consumer = new SimpleConsumer();
        consumer.run(topic);
    }
}
