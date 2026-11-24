from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

import threading
import time
from datetime import datetime
import json


class TestApp(EWrapper, EClient):

    def __init__(self):
        EClient.__init__(self, self)
        self.bid = None
        self.ask = None

    def tickPrice(self, reqId, tickType, price, attrib):

        # BID
        if tickType == 1:
            self.bid = price

        # ASK
        if tickType == 2:
            self.ask = price

        if self.bid is not None and self.ask is not None:

            evento = {
                "symbol": "EURUSD",
                "bid": self.bid,
                "ask": self.ask,
                "timestamp": datetime.utcnow().isoformat()
            }

            print(json.dumps(evento, indent=2))


app = TestApp()

app.connect("127.0.0.1", 7497, clientId=1)

api_thread = threading.Thread(target=app.run, daemon=True)
api_thread.start()

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

print("Esperando eventos...")

time.sleep(20)

app.disconnect()