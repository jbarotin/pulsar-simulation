package org.example;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class ConsumerDriver {
    static final Logger logger = LoggerFactory.getLogger(ConsumerDriver.class.getName());
    private static final List<Consumer> consumerList = new ArrayList<>();
    private static final List<Thread> consumerThreads = new ArrayList<>();
    public static PulsarClient client;

    public static void main(String[] args) throws PulsarClientException {
        final int consumerThreadCount = 10;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Consumer c : consumerList) {
                c.stop();
            }
            for (Thread th : consumerThreads) {
                try {
                    th.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            try {
                client.close();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
            client = PulsarClient.builder()
                    //.ioThreads(consumerThreadCount) //could it be that? (connections to broker)
                    //.connectionsPerBroker(consumerThreadCount) //test
                    .listenerThreads(consumerThreadCount)
                    //.memoryLimit(0, SizeUnit.BYTES)
                    .serviceUrl(org.example.PulsarConfig.brokerUrls).build();

            for (int i = 0; i < consumerThreadCount; ++i) {
                Consumer consumer = new Consumer(client, args[0]);
                Thread thread = new Thread(consumer);
                consumerList.add(consumer);
                consumerThreads.add(thread);
                thread.start();
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
