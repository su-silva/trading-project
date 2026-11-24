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

    if len(ticks) == 10:

        print("\nVela temporal:")

        print("Open:", ticks[0]["bid"])
        print("Close:", ticks[-1]["bid"])

        ticks.clear()