## Introduction

This project aim to simulate some load on Apache Pulsarin a key_shared mode, key have a  

## Build project
```
mvn clean package
```

## Run pulsar standalone
```
docker run -it -p 6650:6650  -p 8080:8080 apachepulsar/pulsar:3.1.1 bin/pulsar standalone
```

## Run simulation 
```
bash run_simulation.sh
```


## Logs analysis query   

get start of the simulation
```
grep idle simulation.consumer1.log simulation.consumer2.log simulation.consumer3.log | sort |head -n 1
```

get end if the simulation
```
grep idle simulation.consumer1.log simulation.consumer2.log simulation.consumer3.log | sort |tail -n 1
```

generate statistics
```
grep -ohP 'thread_idle=\K\d+\,\d*+' simulation.consumer1.log  simulation.consumer2.log simulation.consumer3.log | datamash -H min 1 mean 1 q1 1 median 1 q3 1 max 1
```