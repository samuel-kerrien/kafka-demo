package com.gmail.samuel.kerrien.demo_2;

import com.gmail.samuel.kerrien.KafkaConfig;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerGroupExample {

    private final ConsumerConnector consumer;
    private final String topic;

    private ExecutorService executor;
    private final List<ConsumerThread> threads = new ArrayList<>();

    public ConsumerGroupExample(String zookeeper, String group, String topic) {
        ConsumerConfig consumerConfig = createConsumerConfig(zookeeper, group);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
        this.topic = topic;
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void run(final int threadCount) {
        final Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(threadCount));
        final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now create a thread to consume a partition's messages
        executor = Executors.newFixedThreadPool(threadCount);
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            ConsumerThread task = new ConsumerThread(stream, threadNumber);
            executor.submit(task);
            threads.add(task);
            threadNumber++;
        }
    }

    private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    private void printStats() {
        int total = 0;
        for (ConsumerThread thread : threads) {
            System.out.println("Thread " + thread.getId() + ": " + thread.getMessages() + " messages");
            total += thread.getMessages();
        }
        System.out.println("Total: " + total);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("usage: ConsumerGroupExample <consumer.group> <topic> <threads.count>");
            System.exit(1);
        }

        String groupId = args[0];
        String topic = args[1];
        int threadCount = Integer.parseInt(args[2]);

        System.out.println("Setting up group with " + threadCount + " threads...");
        ConsumerGroupExample consumerGroup = new ConsumerGroupExample(KafkaConfig.ZOOKEEPER, groupId, topic);
        consumerGroup.run(threadCount);

        try {
            final int secondsBeforeShutdown = 20;
            System.out.println("Group will be allowed to consume for " + secondsBeforeShutdown + " seconds...");
            Thread.sleep(secondsBeforeShutdown * 1000);
        } catch (InterruptedException ie) {
        }

        consumerGroup.shutdown();
        consumerGroup.printStats();
    }
}
