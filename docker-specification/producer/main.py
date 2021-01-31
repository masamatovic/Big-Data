#!/usr/bin/python3

import os
import time
import requests
from kafka import KafkaProducer
import kafka.errors
from datetime import datetime

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = "air-pollution"

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)


#slanje aqi-a svakih 10 s na kafku
base_url = "https://api.waqi.info"
token = open('token.waqitoken').read()
city = 'Belgrade'

while True:
    r = requests.get(base_url + f"/feed/{city}/?token={token}")
    message = "City: {}, AQI: {}".format(r.json()['data']['city']['name'], r.json()['data']['aqi'])
    key = r.json()['data']['debug']['sync']
    print(message)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    producer.send(TOPIC, key=bytes( dt_string, 'utf-8'), value=bytes(message, 'utf-8'))
    time.sleep(30)

