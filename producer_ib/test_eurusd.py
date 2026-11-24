from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time

class TestApp(EWrapper, EClient):

    def __init__(self):
        EClient.__init__(self, self)

    def tickPrice(self, reqId, tickType, price, attrib):
        print(f"TickType={tickType} Precio={price}")

app = TestApp()

app.connect("127.0.0.1", 7497, clientId=1)

thread = threading.Thread(target=app.run, daemon=True)
thread.start()

time.sleep(2)

contract = Contract()
contract.symbol = "EUR"
contract.secType = "CASH"
contract.exchange = "IDEALPRO"
contract.currency = "USD"

app.reqMktData(
    1,
    contract,
    "",
    False,
    False,
    []
)

print("Esperando datos de EURUSD...")

time.sleep(30)

app.disconnect()