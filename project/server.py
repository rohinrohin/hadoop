import txaio
from twisted.internet import reactor
from autobahn.twisted.websocket import WebSocketServerFactory, \
	WebSocketServerProtocol, \
	listenWS
from autobahn.websocket.types import ConnectionDeny
import json
from enum import Enum

# helpers

class codes(Enum):
	SUCCESS = 1
	FAIL = 2
	ERR_KEY_ALREADY_EXISTS = 3
	ERR_KEY_NOT_RESPONSIBLE = 4
	ERR_KEY_NOT_FOUND = 5 

def get_multiple(data, keys): 
	resp = {}
	for key in keys: 
		if key in data:
			resp[key] = data[key]
		else: 
			resp[key] = "ERR_KEY_NOT_FOUND"
	return resp
		
def get(data, key):
	if key in data:
		return data[key]
	else: 
		return codes.ERR_KEY_NOT_FOUND 

def put(data, key, value): 
	if False: 
		#todo: key-server mapping check
		return codes.ERR_KEY_NOT_RESPONSIBLE 
	elif key in data:
		return codes.ERR_KEY_ALREADY_EXISTS 

	data[key] = value
	return codes.SUCCESS 

# helpers end

class BaseService:

	def __init__(self, proto):
		self.proto = proto
		self.is_closed = False

	def onOpen(self):
		pass

	def onClose(self, wasClean, code, reason):
		pass

	def onMessage(self, payload, isBinary):
		pass

class KeyStoreService(BaseService):
	
	data = {}
	def onMessage(self, payload, isBinary):
		if not isBinary:
			payload = json.loads(payload.decode('utf-8'))
			payloadType = payload['type'].upper()
			payloadParams = payload['params']
			res = {
				'status': codes.SUCCESS.name
			} 
			if payloadType == 'GET':
				result = get(self.data, payloadParams['key'])
				print("recieved get"+payloadParams['key'])
				print(result)
				if result == codes.ERR_KEY_NOT_FOUND:
					res['status'] = result.name
				else: 
					res['data'] = result
			elif payloadType == 'PUT':
				print("recieved put")
				print(self.data)
				result = put(self.data, payloadParams['key'], payloadParams['value'])
				if result == codes.ERR_KEY_NOT_RESPONSIBLE or result == codes.ERR_KEY_ALREADY_EXISTS:
					res['status'] = result.name
				else:
					res['data'] = result
				
			msg = str(self.data) + "KeyStore Recieved - {}".format(str(payload))
			print(msg)
			self.proto.sendMessage(msg.encode('utf8'))

class MasterService(KeyStoreService):
	
	metadata = {}

	def onMessage(self, payload, isBinary):
		if not isBinary:
			msg = str(self.data) + "Echo master handler - {}".format(payload.decode('utf8'))
			print(msg)
			self.proto.sendMessage(msg.encode('utf8'))
class BackupKeyStoreService(BaseService):

	data = {} 
	def onMessage(self, payload, isBinary):
		if not isBinary:
			msg = "Echo 2 - {}".format(payload.decode('utf8'))
			print(msg)
			self.proto.sendMessage(msg.encode('utf8'))


class ServiceServerProtocol(WebSocketServerProtocol):

	SERVICEMAP = {
		'/master': MasterService,
		'/keystore': KeyStoreService, 
		'/backup': BackupKeyStoreService
	}

	def __init__(self):
		self.service = None
		self.is_closed = txaio.create_future()

	def onConnect(self, request):
		# request has all the information from the initial
		# WebSocket opening handshake ..
		#print(request.peer)
		#print(request.headers)
		#print(request.host)
		#print(request.path)
		#print(request.params)
		#print(request.version)
		#print(request.origin)
		#print(request.protocols)
		#print(request.extensions)

		if request.path in self.SERVICEMAP:
			cls = self.SERVICEMAP[request.path]
			self.service = cls(self)
		else:
			err = "No service under %s" % request.path
			print(err)
			raise ConnectionDeny(404, unicode(err))

	def onOpen(self):
		if self.service:
			self.service.onOpen()

	def onMessage(self, payload, isBinary):
		if type(self.service).__name__ == 'MasterService':
			self.service.onMessage(payload, isBinary)
		elif self.service: 
			self.service.onMessage(payload, isBinary)

	def onClose(self, wasClean, code, reason):
		if self.service:
			self.service.onClose(wasClean, code, reason)


if __name__ == '__main__':

	factory = WebSocketServerFactory(u"ws://127.0.0.1:9000")
	factory.protocol = ServiceServerProtocol
	listenWS(factory)

	reactor.run()