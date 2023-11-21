package org.example;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Producer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class.getName());
    private static final int clientThreadsCount = 10;
    private static final int maxProcessTimeForMessage = 4;
    private int delay = 0;
    private final Thread thread;
    private Runnable callback = null;
    private long callbackTrigger = 0;


    /**
     * Constructor
     * @param keyCount Number of keys
     */
    public Producer(int keyCount) {

        Set<Integer> heavyLoadkeySet = Stream.of(1, 5, 100, 500, 1000, 1600, 2000, 2500, 3000, 5000).collect(Collectors.toSet());
        Set<Integer> mediumLoadkeySet = Stream.of(48,59,283,290,385,396,428,440,457,532,566,673,738,761,805,925,996,1129,1150,1185,
                1238,1344,1362,1454,1463,1467,1469,1478,1487,1555,1558,1562,1686,1690,1698,1709,
                1724,1874,1880,1884,1898,1942,2056,2182,2270,2475,2545,2581,2740,2803,2888,2928,2992,
                3147,3168,3226,3239,3247,3307,3356,3373,3447,3466,3616,3731,3799,3894,3998,4034,4069,4120,
                4149,4195,4259,4366,4420,4450,4453,4456,4576,4643,4655,4679,4806,4869,4885,4937,4995,5028,5103,
                5327,5436,5496,5517,5620,5678,5731,5740,5829,5944,5976).collect(Collectors.toSet()); //for i in $(seq 0 100); do echo $(($RANDOM % 6000+1)), ; done | sort -n

        for(Integer i : heavyLoadkeySet) {
            if (mediumLoadkeySet.contains(i)) {
                throw new RuntimeException("Heavy load key '" + i + "' is also present in the medium load keyset!");
            }
        }

        //1000 msgs on 10 keys
        //100 msgs on 100 keys
        //9 msgs for all the remaining keys (11,890)
        this.thread = new Thread(() -> {
            GenericSchema<GenericRecord> schema = PulsarConfig.getSchema();

            try (PulsarClient client = PulsarClient.builder()
                    .serviceUrl(PulsarConfig.brokerUrls)
                    .ioThreads(clientThreadsCount)
                    .connectionsPerBroker(clientThreadsCount)
                    .build();
                 org.apache.pulsar.client.api.Producer<GenericRecord> producer = client.newProducer(schema)
                         .topic(PulsarConfig.topicName)
                         .create())
            {
                long counter = 0;

                for (int k = 0; k < keyCount; ++k) {
                        final int processTime = 500;

                    if (heavyLoadkeySet.contains(k)) {
                        final int totalMessages = 1000;
                        logger.info("Creating " + totalMessages + " message for key=" + k + "...");

                        for (int m = 0; m < totalMessages; ++m) {
                            GenericRecord value = schema.newRecordBuilder()
                                    .set(PulsarConfig.keyField, k)
                                    .set(PulsarConfig.messageField, m)
                                    .set(PulsarConfig.processTimeField, processTime)
                                    .build();

                            producer.newMessage()
                                    .key(String.valueOf(k))
                                    .value(value)
                                    .send();
                        }
                    } else if (mediumLoadkeySet.contains(k)) {
                        final int totalMessages = 100;
                        logger.info("Creating " + totalMessages + " message for key=" + k + "...");

                        for (int m = 0; m < totalMessages; ++m) {
                            GenericRecord value = schema.newRecordBuilder()
                                    .set(PulsarConfig.keyField, k)
                                    .set(PulsarConfig.messageField, m)
                                    .set(PulsarConfig.processTimeField, processTime)
                                    .build();

                            producer.newMessage()
                                    .key(String.valueOf(k))
                                    .value(value)
                                    .send();
                        }
                    } else {
                        final int totalMessages = 9;
                        logger.info("Creating " + totalMessages + " message for key=" + k + "...");

                        for (int m = 0; m < 9; ++m) {
                            GenericRecord value = schema.newRecordBuilder()
                                    .set(PulsarConfig.keyField, k)
                                    .set(PulsarConfig.messageField, m)
                                    .set(PulsarConfig.processTimeField, processTime)
                                    .build();

                            producer.newMessage()
                                    .key(String.valueOf(k))
                                    .value(value)
                                    .send();
                        }
                    }
                }

                if (this.callback != null && counter < this.callbackTrigger) {
                    this.callback.run();
                }
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void run() throws RuntimeException {
        this.thread.start();
    }

    public void stop() throws RuntimeException {
        logger.info("Stopping producer on thread " + Thread.currentThread());
        try {
            this.thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
