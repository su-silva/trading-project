from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

from kafka import KafkaProducer

import threading
import time
import json
from datetime import datetime


class TickProducer(EWrapper, EClient):

    def __init__(self):
        EClient.__init__(self, self)

        self.bid = None
        self.ask = None

        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def tickPrice(self, reqId, tickType, price, attrib):

        if tickType == 1:
            self.bid = price

        if tickType == 2:
            self.ask = price

        if self.bid is not None and self.ask is not None:

            evento = {
                "symbol": "EURUSD",
                "bid": self.bid,
                "ask": self.ask,
                "timestamp": datetime.utcnow().isoformat()
            }

            self.producer.send("ticks", evento)

            print(evento)


app = TickProducer()

app.connect("127.0.0.1", 7497, clientId=1)

threading.Thread(
    target=app.run,
    daemon=True
).start()

time.sleep(2)

eurusd = Contract()
eurusd.symbol = "EUR"
eurusd.secType = "CASH"
eurusd.exchange = "IDEALPRO"
eurusd.currency = "USD"

app.reqMktData(
    1,
    eurusd,
    "",
    False,
    False,
    []
)

print("Enviando ticks a Kafka...")

time.sleep(60)

app.disconnect()