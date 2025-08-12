from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'samas-social',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consuming SAMAS data...\n")
try:
    for message in consumer:
        print("Received:", message.value)
except KeyboardInterrupt:
    print("Consumer stopped.")