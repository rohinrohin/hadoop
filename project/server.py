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
import math
from websocket import create_connection

# helpers

import sys

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

#following from Python cookbook, #475186
def has_colours(stream):
    if not hasattr(stream, "isatty"):
        return False
    if not stream.isatty():
        return False # auto color only on TTYs
    try:
        import curses
        curses.setupterm()
        return curses.tigetnum("colors") > 2
    except:
        # guess false in case of error
        return False
has_colours = has_colours(sys.stdout)


def printout(text, colour=WHITE):
        text = '{:<20}'.format(text)
        if has_colours:
                seq = "\x1b[1;%dm" % (30+colour) + text + "\x1b[0m"
                sys.stdout.write(seq)
        else:
                sys.stdout.write(text)

class codes(Enum):
    SUCCESS = 1
    FAIL = 2
    ERR_KEY_ALREADY_EXISTS = 3
    ERR_KEY_NOT_RESPONSIBLE = 4
    ERR_KEY_NOT_FOUND = 5
    ERR_SERVER_NOT_INIT = 6

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
    keyRange = {"status": "false", "range": ""}
    test = 0

    def onMessage(self, payload, isBinary):
        if not isBinary:
            payload = json.loads(payload.decode('utf-8'))
            printout("[SLAVE SERVER]", YELLOW)
            print("RECIEVED: " + str(payload))
            payloadType = payload['type'].upper()
            payloadParams = payload['params']
            res = {
                'status': str(codes.SUCCESS.name)
            }

            if payloadType == 'SETKEY':
                print("KEYSET RECIEVED FROM MASTER")
                self.keyRange["range"] = payloadParams['data']
                self.keyRange["status"] = "true"
                print("KEYRANGE: ", self.keyRange)
                msg = json.dumps(res)
                print("SERVER SENT: " + msg)
                self.proto.sendMessage(msg.encode('utf8'))
                return

            if self.keyRange["status"] == "false":
                res['status'] = str(codes.ERR_SERVER_NOT_INIT.name)

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
                firstChar = ord(payloadParams['key'][0])
                start, end = self.keyRange['range'].split('-')
                start, end = int(start), int(end)
                if firstChar > start and firstChar <= end:
                    result = put(self.data, payloadParams['key'], payloadParams['value'])
                else:
                    res['status'] = codes.ERR_KEY_NOT_RESPONSIBLE

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
    keyRange = {"status": "false", "range": ""}
    keyRanges = {}

    def checkKeyRange(self, key):
        start, end = self.keyRange['range'].split('-')
        start, end = int(start), int(end)
        firstChar = ord(key[0])
        if not (firstChar > start and firstChar <= end):
            for key in self.keyRanges:
                start, end = key.split('-')
                start, end = int(start), int(end)
                if firstChar > start and firstChar <= end:
                    return self.keyRanges[key]

        return (codes.SUCCESS)

    def onMessage(self, payload, isBinary):
        if not isBinary:
            payload = json.loads(payload.decode('utf-8'))
            print("MASTER SERVER RECEIVED: " + str(payload))
            payloadType = payload['type'].upper()
            payloadParams = payload['params']
            res = {
                'status': str(codes.SUCCESS.name)
            }

            if self.keyRange["status"] == "false":
                res['status'] = str(codes.ERR_SERVER_NOT_INIT.name)

            if payloadType == 'GET':
                keyRangeCheck = self.checkKeyRange(payloadParams['key'])
                if keyRangeCheck == codes.SUCCESS:
                    # key in master
                    result = get(self.data, payloadParams['key'])
                    if result == codes.ERR_KEY_NOT_FOUND:
                        # key supposed to be in master but not found
                        res['status'] = str(result.name)
                    else:
                        res['data'] = result
                else:
                    # key not responsible
                    res["status"] = str(codes.ERR_KEY_NOT_RESPONSIBLE.name)
                    res["data"] = str(keyRangeCheck)
                print(res)

            #elif payloadType == 'GETMULTIPLE':
            #    result = {}
            #    for key in payloadParams['keys']:
            #        temp = get(self.data, key)
            #        if temp == codes.ERR_KEY_NOT_FOUND:
            #            result[key] = str(temp.name)
            #        else:
            #            result[key] = temp
            #        res['data'] = result

            elif payloadType == 'PUT':
                keyRangeCheck = self.checkKeyRange(payloadParams['key'])
                if keyRangeCheck == codes.SUCCESS:
                    # key in master
                    result = put(self.data, payloadParams['key'], payloadParams['value'])
                    if result == codes.ERR_KEY_ALREADY_EXISTS:
                        res['status'] = str(result.name)
                else:
                    res["status"] = str(codes.ERR_KEY_NOT_RESPONSIBLE.name)
                    res["data"] = str(keyRangeCheck)

            elif payloadType == 'REPLICA':
                res['data'] = self.data


            else:
                res['status'] = str(codes.FAIL.name)
                res['data'] = "Operation not permitted"

            msg = json.dumps(res)
            print("SERVER SENT: " + msg)
            self.proto.sendMessage(msg.encode('utf8'))

class BackupKeyStoreService(BaseService):

    isMaster = {"flag": "false"}
    data = {}
    keyRange = {"status": "false", "range": ""}
    keyRanges = {}

    def checkKeyRange(self, key):
        start, end = self.keyRange['range'].split('-')
        start, end = int(start), int(end)
        firstChar = ord(key[0])
        if not (firstChar > start and firstChar <= end):
            for key in self.keyRanges:
                start, end = key.split('-')
                start, end = int(start), int(end)
                if firstChar > start and firstChar <= end:
                    return self.keyRanges[key]

        return (codes.SUCCESS)

    def onMessage(self, payload, isBinary):
        if not isBinary:
            payload = json.loads(payload.decode('utf-8'))
            payloadType = payload['type'].upper()
            payloadParams = payload['params']
            res = {
                'status': str(codes.SUCCESS.name)
            }

            if payloadType == 'SETKEY':
                print("KEYSET RECIEVED")
                self.keyRange["range"] = payloadParams['data']
                self.keyRange["status"] = "true"

                if "master" in payloadParams:
                    # check if backup of master
                    self.keyRanges = payload["keyRanges"]
                    isMaster["flag"] = True

                print("BACKUP KEYRANGE: ", self.keyRange)
                msg = json.dumps(res)
                print("SERVER SENT: " + msg)
                self.proto.sendMessage(msg.encode('utf8'))
                return

            # moved print statement here to to know if master
            # backup or keystore backup
            print("MASTER" if self.isMaster["flag"] else "KEYSTORE" + "BACKUP SERVER RECEIVED: " + str(payload))

            if self.keyRange["status"] == "false":
                res['status'] = str(codes.ERR_SERVER_NOT_INIT.name)

            if payloadType == 'GET':
                keyRangeCheck = self.checkKeyRange(payloadParams['key'])
                if keyRangeCheck == codes.SUCCESS:
                    # key in master
                    result = get(self.data, payloadParams['key'])
                    if result == codes.ERR_KEY_NOT_FOUND:
                        # key supposed to be in master but not found
                        res['status'] = str(result.name)
                    else:
                        res['data'] = result
                else:
                    # key not responsible
                    res["status"] = str(codes.ERR_KEY_NOT_RESPONSIBLE.name)
                    res["data"] = str(keyRangeCheck)
                print(res)

            #elif payloadType == 'GETMULTIPLE':
            #    result = {}
            #    for key in payloadParams['keys']:
            #        temp = get(self.data, key)
            #        if temp == codes.ERR_KEY_NOT_FOUND:
            #            result[key] = str(temp.name)
            #        else:
            #            result[key] = temp
            #        res['data'] = result

            elif payloadType == 'PUT':
                keyRangeCheck = self.checkKeyRange(payloadParams['key'])
                if keyRangeCheck == codes.SUCCESS:
                    # key in master
                    result = put(self.data, payloadParams['key'], payloadParams['value'])
                    if result == codes.ERR_KEY_ALREADY_EXISTS:
                        res['status'] = str(result.name)
                else:
                    res["status"] = str(codes.ERR_KEY_NOT_RESPONSIBLE.name)
                    res["data"] = str(keyRangeCheck)

            elif payloadType == 'REPLICA':
                res['data'] = self.data


            else:
                res['status'] = str(codes.FAIL.name)
                res['data'] = "Operation not permitted"

            msg = json.dumps(res)
            print("SERVER SENT: " + msg)
            self.proto.sendMessage(msg.encode('utf8'))
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
            print("MASTER")
            if zk.exists('/meta'):
                zk.delete('/meta', recursive=True)

            zk.create('/meta/master',b'8080', makepath=True)
            zk.create('/meta/status',b'initializing', makepath=True)
            zk.create('/meta/lastport','8080'.encode('utf-8'), makepath=True)
            time.sleep(5)
            children = zk.get_children('/app')
            config = {
                "lastDead": {
                    "backup": -1,
                    # backup is the server port whose data the dead server needs to backuo
                    "keystore": -1
                    # keystore is the server whose /backup it has to read from
                },
                "numOfServers": len(children)
            }
            zk.create('/meta/config', json.dumps(config).encode('utf-8'))
            print('Server init')
            print(children)
            totalKeyRange = 122 - 48 + 1
            individualRange = int(math.floor(totalKeyRange / len(children)))
            ranges = []
            start = 48
            for i in range(len(children)):
                ranges.append(str(start) + "-" + str(start+individualRange))
                start = start + individualRange + 1
            ranges[-1] = ranges[-1].split("-")[0] + "-122" # change this later please
            print(ranges)
            MasterService.keyRange["range"] = ranges[0]
            MasterService.keyRange["status"] == "True"

            portnum, _ = zk.get('/meta/lastport')
            portnum = int(portnum.decode('utf-8'))
            for port in range(8081, portnum + 1):
               MasterService.keyRanges[ranges[port - 8080]] = port

               signal = {
                   "type": "setkey",
                   "params": {"data": ranges[port - 8080]}
               }

               ws = create_connection("ws://127.0.0.1:" + str(port) + "/keystore")
               print(json.dumps(signal))
               ws.send(json.dumps(signal))
               print ("Sent")
               print ("Receiving...")
               result =  ws.recv()
               print ("Received '%s'" % result)
               ws.close()

    else:
        print("SLAVE")
        portno, _ = zk.get('/meta/lastport')
        portno = int(portno.decode('utf-8')) + 1
        zk.set('/meta/lastport', str(portno).encode())
        print("LISTENING FOR MASTER KEYSET")

    factory = WebSocketServerFactory(u"ws://127.0.0.1:%s" % str(portno))
    factory.protocol = ServiceServerProtocol
    listenWS(factory)

    reactor.run()
