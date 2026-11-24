from ibapi.client import EClient
from ibapi.wrapper import EWrapper

class TestApp(EWrapper, EClient):

    def __init__(self):
        EClient.__init__(self, self)

app = TestApp()

print("Intentando conectar a TWS...")

app.connect("127.0.0.1", 7497, clientId=1)

print("Conexión realizada")

app.disconnect()

print("Finalizado")