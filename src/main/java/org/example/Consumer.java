package org.example;

import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;



public class Consumer implements Runnable {
    static final Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
    private final PulsarClient client;
    private org.apache.pulsar.client.api.Consumer<GenericRecord> pulsarConsumer;
    private static ThreadLocal<Timer> idle = new ThreadLocal<>();
    private final AtomicBoolean runState = new AtomicBoolean(true);

    /**
     * Constructor
     * @param client Pulsar client for consumer
     */
    public Consumer(PulsarClient client, String clientId) {
        this.client = client;
    }

    @Override
    public void run() throws RuntimeException {
        idle.set(new Timer());

        try {
            org.apache.pulsar.client.api.Consumer<GenericRecord> pulsarConsumer = this.client.newConsumer(PulsarConfig.getSchema())
                    .topic(PulsarConfig.topicName)
                    .consumerName("TestConsumer" + Thread.currentThread().getId())
                    .subscriptionName(PulsarConfig.subscriptionName)
                    .subscriptionMode(SubscriptionMode.Durable)
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .keySharedPolicy(KeySharedPolicy.autoSplitHashRange().setAllowOutOfOrderDelivery(true))
                    .receiverQueueSize(500)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .messageListener((MessageListener<GenericRecord>) (consumer, msg) -> {
                        final GenericRecord genericRecord = PulsarConfig.getSchema().decode(msg.getData());
                        final int processingTime = Integer.parseInt(String.valueOf(genericRecord.getField(PulsarConfig.processTimeField)));

                        Timer processingTimer = new Timer();
                        logger.info("(Thread " + Thread.currentThread().getId() + ") Starting process " +
                                "k=" + genericRecord.getField(PulsarConfig.keyField) + " " +
                                "m=" + genericRecord.getField(PulsarConfig.messageField) + " " +
                                "p=" + genericRecord.getField(PulsarConfig.processTimeField) + " " +
                                "thread_idle=" + (idle.get() != null ? idle.get().toString() : "n/a"));
                        try {
                            consumer.acknowledge(msg);
                        } catch (PulsarClientException e) {
                            throw new RuntimeException(e);
                        }

                       if (processingTime > 0) {
                            try {
                                Thread.sleep(processingTime);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        logger.info("(Thread " + Thread.currentThread().getId() + ") Finished process time:" + processingTimer);
                        Consumer.idle.set(new Timer());
                    }).subscribe();

        } catch(PulsarClientException e) {
            throw new RuntimeException(e);

        } finally {
            Runtime.getRuntime().addShutdownHook(new Thread(()-> {
                try {
                    this.pulsarConsumer.close();
                    this.client.close();
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
    }

    public void stop() {
        logger.info("Stopping consumer on thread " + Thread.currentThread());
        this.runState.set(false);
    }
}
