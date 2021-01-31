#!/bin/bash


winpty docker exec -it namenode bash -c " rm -rf home/results/r2017"
winpty docker exec -it namenode bash -c "cd home/results && hdfs dfs -get r2017"
python plot.py

