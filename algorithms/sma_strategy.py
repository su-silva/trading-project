from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

consumer = KafkaConsumer(
    "velas",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

closes = []

SMA_CORTA = 3
SMA_LARGA = 5

print("Escuchando velas...")

for mensaje in consumer:

    vela = mensaje.value

    close = vela["close"]

    closes.append(close)

    print(f"Close recibido: {close}")

    if len(closes) >= SMA_LARGA:

        sma_corta = sum(closes[-SMA_CORTA:]) / SMA_CORTA
        sma_larga = sum(closes[-SMA_LARGA:]) / SMA_LARGA

        print(
            f"SMA{SMA_CORTA}: {sma_corta:.5f} | "
            f"SMA{SMA_LARGA}: {sma_larga:.5f}"
        )

        signal = None

        if sma_corta > sma_larga:
            signal = "BUY"

        elif sma_corta < sma_larga:
            signal = "SELL"

        if signal:

            evento = {
                "id_algoritmo": "SMA_CROSS",
                "symbol": "EURUSD",
                "signal": signal,
                "precio_referencia": close,
                "timestamp": datetime.utcnow().isoformat()
            }

            producer.send("signals", evento)
            producer.flush()

            print("\nSIGNAL GENERADA")
            print(evento)
            print()