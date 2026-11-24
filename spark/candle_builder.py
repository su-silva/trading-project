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
	
	precios = [tick["bid"] for tick in ticks]

        print("\nVela temporal:")

        print("Open:", precios[0])
	print("High:", max(precios))
	print("Low:", min(precios))
        print("Close:", precios[-1])
	print("num_ticks:", len(precios))

        ticks.clear()