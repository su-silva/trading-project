from kafka import KafkaConsumer
import json
consumer = KafkaConsumer(
    "ticks",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Escuchando topic ticks...")

for mensaje in consumer:
    print(mensaje.value)