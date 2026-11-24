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

        vela = {
            "symbol": "EURUSD",
            "open": precios[0],
            "high": max(precios),
            "low": min(precios),
            "close": precios[-1],
            "num_ticks": len(precios)
        }

        print("\nVela generada:")
        print(vela)

        ticks.clear()