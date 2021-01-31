#!/bin/bash


docker cp datasets/ namenode:/home/


winpty docker exec -it namenode bash -c "hdfs dfs -rm -r -f /air-pollution*"
winpty docker exec -it namenode bash -c "hdfs dfs -mkdir /air-pollution"

winpty docker exec -it namenode bash -c "hdfs dfs -put home/datasets/2017_india.csv  /air-pollution/"
winpty docker exec -it namenode bash -c "hdfs dfs -put home/datasets/2016_india.csv  /air-pollution/"
winpty docker exec -it namenode bash -c "hdfs dfs -put home/datasets/deaths.csv  /air-pollution/"

