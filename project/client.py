import sys
import ast

from twisted.internet import reactor
from twisted.python import log

from autobahn.twisted.websocket import WebSocketClientFactory, \
	WebSocketClientProtocol, \
	connectWS
import json


class ClientProtocol(WebSocketClientProtocol):

	def sendHello(self):
		testput = {
				"type": "put",
				"params": {
						"key": "hello",
						"value": "world"
				}
		}
		testput2 = {
				"type": "put",
				"params": {
						"key": "hello1",
						"value": "world1"
				}
		}

		testget = {
				"type": "get",
				"params": {
						"key": "hello"
				}
		}

		testmultiple = {
				"type": "getmultiple",
				"params": {
						"keys": ["hello", "hello1", "testfail"] 
				}
		}

		request = input().split()
		#if len(request) > 3:
			# hack to allow spaces in json value input
		#	request = [request[0],request[1],''.join(request[2:])]
		method, params = request[0].lower(), ""
		if method == "get":
			params = {
				"key": request[1]
			}
		elif method == "put":
			value = ''.join(request[2:])
			if value[0] == '{':
				value = literal_eval(value)
			else: 
				value = value.strip()
			params = {
				"key": request[1],
				"value": value
			}
		elif method == "getmultiple":
			params = {
				"keys": request[1:]
			}
		message = {
			"type": method.lower(),
			"params": params
		}
		self.sendMessage(json.dumps(message).encode('utf8'))
		#self.sendMessage(json.dumps(testput).encode('utf8'))
		#self.sendMessage(json.dumps(testget).encode('utf8'))

	def onOpen(self):
		self.sendHello()

	def onClose(self, wasClean, code, reason):
		print(reason)

	def onMessage(self, payload, isBinary):
		if not isBinary:
			print("RESPONSE: {}".format(payload.decode('utf8')))
		reactor.callLater(1, self.sendHello)


class ClientFactory(WebSocketClientFactory):

	protocol = ClientProtocol

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

	factory = ClientFactory(sys.argv[1])
	connectWS(factory)

	reactor.run()

