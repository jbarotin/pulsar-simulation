package org.example;

import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerDriver {
    static final Logger logger = LoggerFactory.getLogger(ProducerDriver.class.getName());
    public static void main(String[] args) throws PulsarClientException {
        final int keyCount = 6000;
        final Producer producer = new Producer(keyCount);


        Runtime.getRuntime().addShutdownHook(new Thread(producer::stop));
        producer.run();
    }
}
