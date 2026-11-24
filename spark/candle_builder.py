from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "ticks",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

ticks = []

print("Escuchando ticks...")

for mensaje in consumer:

    tick = mensaje.value

    ticks.append(tick)

    print(f"Ticks acumulados: {len(ticks)}")