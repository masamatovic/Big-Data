#!/bin/bash


docker cp batch.py spark-master:/home/
winpty docker exec -it spark-master bash -c "spark/bin/spark-submit home/batch.py"

#winpty docker exec -it namenode bash -c "hdfs dfsadmin -safemode leave"
#winpty docker exec -it namenode bash -c "chmod +x /home/results/run.sh && /home/results/run.sh"


