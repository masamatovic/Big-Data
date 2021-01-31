#!/bin/bash


#winpty docker exec -it namenode bash -c " rm -rf home/results/r2017"
#winpty docker exec -it namenode bash -c "home/results hdfs dfs -get r2017"

DIR=/home/results

cd $DIR

rm -rf r2017
hdfs dfs -get /r2017/*