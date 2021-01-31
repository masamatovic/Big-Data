#!/bin/bash

docker cp AQI-stream.py spark-master:/spark/
winpty docker exec -it spark-master bash -c "spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 spark/AQI-stream.py zoo1:2181 air-pollution"