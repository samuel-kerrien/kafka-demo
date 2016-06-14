package com.gmail.samuel.kerrien;


import java.nio.charset.StandardCharsets;
import java.util.*;

import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import kafka.admin.TopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;

import static org.junit.Assert.*;

public class KafkaTest {

    private String topic = "test";
    private EmbeddedZookeeper zkServer;

    private ZkClient zkClient;

    private int brokerId = 0;
    private int brokerPort;
    private KafkaServer kafkaServer;

    private ConsumerConnector consumer;

    @Before
    public void setUp() {
        // setup Zookeeper
        String zkConnect = TestZKUtils.zookeeperConnect();
        System.out.println("zkConnect = " + zkConnect);
        zkServer = new EmbeddedZookeeper(zkConnect);
        zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        // setup Broker
        brokerPort = TestUtils.choosePort();
        System.out.println("broker brokerPort = " + brokerPort);
        Properties props = TestUtils.createBrokerConfig(brokerId, brokerPort, true);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        System.out.println("Kafka broker started.");

        String[] arguments = new String[]{"--topic", topic, "--partitions", "1", "--replication-factor", "1"};
        // create topic
        TopicCommand.createTopic(zkClient, new TopicCommand.TopicCommandOptions(arguments));
        System.out.println("Topic created.");

        List<KafkaServer> servers = new ArrayList<KafkaServer>();
        servers.add(kafkaServer);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);
    }

    @After
    public void tearDown() {
        consumer.shutdown();
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }

    @Test
    public void producerTest() throws InterruptedException {
        // setup producer
        Properties properties = TestUtils.getProducerConfig("localhost:" + brokerPort);
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer producer = new Producer(producerConfig);

        // setup simple consumer
        final String consumerGroupId = "group0";
        Properties consumerProperties = TestUtils.createConsumerProperties(zkServer.connectString(), consumerGroupId, "consumer0", -1);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));

        // send message
        final String sentMessage = "test-message " + UUID.randomUUID().toString();
        KeyedMessage<Integer, byte[]> data = new KeyedMessage(topic, sentMessage.getBytes(StandardCharsets.UTF_8));
        List<KeyedMessage> messages = new ArrayList<KeyedMessage>();
        messages.add(data);
        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        producer.close();

        // deleting zookeeper information to make sure the consumer starts from the beginning
        // see https://stackoverflow.com/questions/14935755/how-to-get-data-from-old-offset-point-in-kafka
        zkClient.delete("/consumers/" + consumerGroupId);

        // starting consumer
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        if (iterator.hasNext()) {
            String msg = new String(iterator.next().message(), StandardCharsets.UTF_8);
            System.out.println(msg);
            assertEquals(sentMessage, msg);
        } else {
            fail("Expected a message in topic: " + topic);
        }
    }
}
