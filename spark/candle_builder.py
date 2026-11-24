from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    "ticks",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
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
            "inicio": ticks[0]["timestamp"],
            "open": precios[0],
            "high": max(precios),
            "low": min(precios),
            "close": precios[-1],
            "num_ticks": len(precios)
        }

        print("\nVela generada:")
        print(vela)

        ticks.clear()