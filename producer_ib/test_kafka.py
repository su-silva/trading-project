from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

evento = {
    "symbol": "EURUSD",
    "bid": 1.1,
    "ask": 1.2,
    "timestamp": "test"
}

producer.send("ticks", evento)
producer.flush()

print("Mensaje enviado a Kafka")