#!/bin/bash

java -cp target/pulsar_simulation.jar:target/libs/* org.example.ProducerDriver > simulation.producer.log

java -cp target/pulsar_simulation.jar:target/libs/* org.example.ConsumerDriver c1 > simulation.consumer1.log &
java -cp target/pulsar_simulation.jar:target/libs/* org.example.ConsumerDriver c2 > simulation.consumer2.log &
java -cp target/pulsar_simulation.jar:target/libs/* org.example.ConsumerDriver c3 > simulation.consumer3.log &

wait

echo "test finished"
