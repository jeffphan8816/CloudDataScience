from kafka import KafkaProducer
from flask import current_app, request
import json

def main():
    producer = KafkaProducer(bootstrap_servers='kafka-kafka-bootstrap.kafka.svc.cluster.local:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for i in range(10):
        current_app.logger.info(f'sending message {i}')
        producer.send('test-topic', {'foo': 'bar'})
        producer.flush()
    return "done"