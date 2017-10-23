import txaio
from twisted.internet import reactor
from autobahn.twisted.websocket import WebSocketServerFactory, \
	WebSocketServerProtocol, \
	listenWS
from autobahn.websocket.types import ConnectionDeny
import json
from enum import Enum
from kazoo.client import KazooClient
import time

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
			print("SERVER RECIEVED: " + str(payload))
			payloadType = payload['type'].upper()
			payloadParams = payload['params']
			res = {
				'status': str(codes.SUCCESS.name).lower()
			}

			if payloadType == 'GET':
				result = get(self.data, payloadParams['key'])
				print(result)
				if result == codes.ERR_KEY_NOT_FOUND:
					res['status'] = str(result.name)
				else:
					res['data'] = result

			elif payloadType == 'GETMULTIPLE':
				result = {}
				for key in payloadParams['keys']:
					temp = get(self.data, key)
					if temp == codes.ERR_KEY_NOT_FOUND:
						result[key] = str(temp.name)
					else:
						result[key] = temp
					res['data'] = result

			elif payloadType == 'PUT':
				result = put(self.data, payloadParams['key'], payloadParams['value'])
				if result == codes.ERR_KEY_NOT_RESPONSIBLE or result == codes.ERR_KEY_ALREADY_EXISTS:
					res['status'] = str(result.name)

			elif payloadType == 'REPLICA':
				res['data'] = self.data

			else:
				res['status'] = str(codes.FAIL.name)
				res['data'] = "Operation not permitted"

			msg = json.dumps(res)
			print("SERVER SENT: " + msg)
			self.proto.sendMessage(msg.encode('utf8'))

class MasterService(BaseService):

	metadata = {}
	data = {}

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

	zk = KazooClient(hosts='192.168.31.233:2181')
	zk.start()

	import logging
	logging.basicConfig()

	currInstance = zk.create('/app/instance', ephemeral=True, sequence=True, makepath=True)
	print(currInstance, " started. ")

	children = zk.get_children('/app')
	print(children)

	portno = 0

	if len(children) == 1:
			portno = 8080
			print("SERVER")
			if zk.exists('/meta'):
				zk.delete('/meta', recursive=True)

			zk.create('/meta/master',b'8080', makepath=True)
			zk.create('/meta/status',b'initializing', makepath=True)
			zk.create('/meta/lastport','8080'.encode('utf-8'), makepath=True)
			time.sleep(20)
			children = zk.get_children('/app')
			config = {
				"lastDead": {
					"backup": -1,
					# backup is the server port who's data the dead server needs to backuo
					"keystore": -1
					# keystore is the server who's '/backup it has to read from
				},
				"numOfServers": len(children)
			}
			zk.create('/meta/config', json.dumps(config).encode('utf-8'))
			print('Server init')
			print(children)
			totalKeyRange = 122 - 48 + 1
			individualRange = totalKeyRange / len(children)
			keyRanges = []
			#for i in range(totalKeyRange/individualRange):
			#	keyRanges


			MasterService.keymapping =

	else:
		print("SLAVE")
		portno, _ = zk.get('/meta/lastport')
		portno = int(portno.decode('utf-8')) + 1
		zk.set('/meta/lastport', str(portno).encode())


	factory = WebSocketServerFactory(u"ws://127.0.0.1:%s" % str(portno))
	factory.protocol = ServiceServerProtocol
	listenWS(factory)

	reactor.run()
