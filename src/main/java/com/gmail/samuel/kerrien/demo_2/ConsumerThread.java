package com.gmail.samuel.kerrien.demo_2;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ConsumerThread implements Runnable {
    private final KafkaStream stream;
    private final int threadNumber;
    private final int id;
    private int messages = 0;

    public ConsumerThread(KafkaStream stream, int threadNumber) {
        this.threadNumber = threadNumber;
        this.stream = stream;
        this.id = threadNumber;
    }

    public int getMessages() {
        return messages;
    }

    public int getId() {
        return id;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println("Thread " + threadNumber + ": " + new String(it.next().message()));
            messages++;
        }
        System.out.println(String.format( "Thread %d: %,d messages", threadNumber, messages ));
        System.out.println("Shutting down Thread: " + threadNumber);
    }
}
