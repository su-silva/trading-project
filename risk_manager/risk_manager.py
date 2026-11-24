from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "signals",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

ultima_signal = None

print("Risk Manager iniciado")

for mensaje in consumer:

    signal = mensaje.value

    if signal["signal"] == ultima_signal:
        continue

    ultima_signal = signal["signal"]

    orden = {
        "symbol": signal["symbol"],
        "accion": signal["signal"],
        "precio": signal["precio_referencia"]
    }

    print("\nORDEN APROBADA")
    print(orden)