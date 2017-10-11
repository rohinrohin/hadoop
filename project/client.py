import sys

from twisted.internet import reactor
from twisted.python import log

from autobahn.twisted.websocket import WebSocketClientFactory, \
	WebSocketClientProtocol, \
	connectWS
import json


class EchoClientProtocol(WebSocketClientProtocol):

	def sendHello(self):
		#self.sendMessage(str(json.dumps({'hello': 'world'})))
		testput = {
				"type": "put",
				"params": {
						"key": "hello",
						"value": "world"
				}
		}
		testget = {
				"type": "get",
				"params": {
						"key": "hello"
				}
		}
		self.sendMessage(json.dumps(testput).encode('utf8'))
		self.sendMessage(json.dumps(testget).encode('utf8'))

	def onOpen(self):
		self.sendHello()

	def onClose(self, wasClean, code, reason):
		print(reason)

	def onMessage(self, payload, isBinary):
		if not isBinary:
			print("Text message received: {}".format(payload.decode('utf8')))
		reactor.callLater(1, self.sendHello)


class EchoClientFactory(WebSocketClientFactory):

	protocol = EchoClientProtocol

	def clientConnectionLost(self, connector, reason):
		print(reason)
		reactor.stop()

	def clientConnectionFailed(self, connector, reason):
		print(reason)
		reactor.stop()


if __name__ == '__main__':

	if len(sys.argv) < 2:
		print("Need the WebSocket server address, i.e. ws://127.0.0.1:9000/echo1")
		sys.exit(1)

	factory = EchoClientFactory(sys.argv[1])
	connectWS(factory)

	reactor.run()

