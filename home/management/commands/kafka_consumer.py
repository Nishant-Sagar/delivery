from django.core.management.base import BaseCommand
from confluent_kafka import Consumer, KafkaError
from home.models import LocationUpdate
import json
import os

class Command(BaseCommand):
    help = 'Consume messages from Kafka topic'

    def handle(self, *args, **options):
        conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id' : 'location_group',
            'auto.offset.reset' : 'earliest',
            }
        
        consumer = Consumer(conf)
        consumer.subscribe(['tracking'])

        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print("Consumer error: {}".format(msg.error()))
                        break
                message = json.loads(msg.value().decode('utf-8'))
                print(message)
                LocationUpdate.objects.create(latitude=message['latitude'], longitude=message['longitude'])
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()