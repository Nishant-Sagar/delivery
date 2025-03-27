from confluent_kafka import Producer
import json
import os
import numpy as np
import time

conf = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(**conf)

# Example Usage
start_lat, start_lon = 12.9716, 77.5946  # Bangalore
end_lat, end_lon = 28.7041, 77.1025  # Delhi
steps = 5000
delta_lat = (end_lat - start_lat) / steps
delta_lon = (end_lon - start_lon) / steps
current_steps = 0

topic = 'tracking'

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

while True:
    latitude = start_lat + current_steps * delta_lat
    longitude = start_lon + current_steps * delta_lon
    message = {
        "latitude": latitude,
        "longitude": longitude
    }
    print(message)
    producer.produce(topic, key='tracking', value=json.dumps(message).encode('utf-8'), callback = delivery_report)
    if current_steps % 10 == 0:
        producer.flush()
    current_steps += 1
    if current_steps > steps:
        current_steps = 0

    time.sleep(2)




