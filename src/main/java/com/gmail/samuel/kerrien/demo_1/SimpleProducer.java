package com.gmail.samuel.kerrien.demo_1;

import com.gmail.samuel.kerrien.KafkaConfig;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;

public class SimpleProducer {

    private final Producer<String, String> producer;

    public SimpleProducer() {
        final Properties properties = buildProperties();
        final ProducerConfig config = new ProducerConfig(properties);
        producer = new Producer<>(config);
    }

    private Properties buildProperties() {
        final Properties properties = new Properties();
        properties.put("metadata.broker.list", KafkaConfig.SEED_BROKERS);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        return properties;
    }

    private void run(String topic, int messageCount) {
        final long start = System.currentTimeMillis();
        for (int i = 0; i < messageCount; i++) {
            final String msg = "{ \"n\": \"" + i + "\", \"d:\" \"" + new Date().toString() + "\" }";

            // no key specified here
            final KeyedMessage<String, String> message = new KeyedMessage<>(topic, msg);

            producer.send(message);

            if ((i % 5000) == 0) {
                float throughput = ((float) i) / ((System.currentTimeMillis() - start) / 1000);
                System.out.println(String.format("Wrote %,d [throughput: %,.2f msg/sec]", i, throughput));
            }
        }
        final long end = System.currentTimeMillis();
        float elapsedInSeconds = (float) (end - start) / (float) 1000;
        System.out.println("elapsedInSeconds = " + elapsedInSeconds);
        System.out.println(String.format("#messages: %,d  throughput: %.3f messages/second",
                                         messageCount,
                                         ((float) messageCount / elapsedInSeconds)));
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("usage: SimpleProducer <topic> <message.count>");
            System.exit(1);
        }

        final String topic = args[0];
        final int messageCount = Integer.parseInt(args[1]);

        System.out.println("topic = " + topic);
        System.out.println("messageCount = " + messageCount);

        SimpleProducer producer = new SimpleProducer();
        producer.run(topic, messageCount);
    }
}
