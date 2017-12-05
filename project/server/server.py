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
import sched, time

s = sched.scheduler(time.time, time.sleep)

PORT_NO = ""
NUMBER_SERVERS = ""

# helpers

persistList = list()

zk = KazooClient(hosts='192.168.31.233:2181')
zk.start()

def scheduleChildWatcher():
    @zk.ChildrenWatch("/app")
    def watch_children(children):
        global persistList

        nodelist = []
        for child in children:
            if 'instance' in child:
                nodelist.append(child)

        # print(persistList)
        # print(nodelist)

        persistSet = set(persistList)
        childSet = set(nodelist)

        deadSet = persistSet - childSet

        if (len(deadSet) and len(persistSet)>len(childSet)):
            # something died
            config, _ = zk.get("/meta/config")
            config = json.loads(config.decode("utf-8"))
            print(deadSet)
            deadInstance = str(list(deadSet)[0])
            deadInstancePort = config["mapper"][deadInstance]
            deadInstanceBackup = 8080 + ((abs(deadInstancePort-8080) +1) % config["numOfServers"])
            if deadInstancePort == 8080:
                # master died
                if portno == deadInstanceBackup:
                    # master backup slave
                    print("NOTIFICATION:", deadSet, " [master] died. ")
                    print("SLAVE PERSISTING DEAD MASTER CONFIG")
                    config["lastDead"]["portno"] = deadInstancePort
                    config["lastDead"]["backup"] = deadInstanceBackup
            else:
                # slave died
                print("NOTIFICATION:", deadSet, " [slave] died. ")
                masterPort, _ = zk.get('meta/master')
                masterPort = masterPort.decode("utf-8")
                if portno == int(masterPort):
                    #inside master
                    print("MASTER PERSISTING DEAD SLAVE CONFIG")
                    config["lastDead"]["portno"] = deadInstancePort
                    config["lastDead"]["backup"] = deadInstanceBackup
                    # set keyrange for backup server
                    for key, value in MasterService.keyRanges.items():
                        if value == deadInstancePort:
                            MasterService.keyRanges[key] = str(deadInstanceBackup) + "/backup"

            del config["mapper"][deadInstance] # remove now reduntant instance
            zk.set('/meta/config', json.dumps(config).encode('utf-8'))
            persistList = nodelist.copy()


def scheduleSignals(a='default'):
    print("Start Signals Scheduled")
    print("Child Watcher Scheduled")
    scheduleChildWatcher()
    if portno == 8080:
        for port in range(8081, portnum + 1):
            MasterService.keyRanges[ranges[port - 8080]] = port

            signal = {
               "type": "setkey",
               "params": {"data": ranges[port - 8080]}
            }

            ss = create_connection("ws://localhost:" + str(port) + "/keystore")
            printout("[MASTER]", RED)
            print("SENDING SIGNAL TO (" + str(port) + "): ", json.dumps(signal))
            ss.send(json.dumps(signal))
            #result =  ss.recv()
            ss.close()
            printout("[MASTER]", RED)
            print ("RECIEVED ACK. ")
            time.sleep(10)

def scheduleMasterKeySet():
    # master keyset and propogate to backup
    print("MASTER KEYSET RUNNING")
    MasterService.keyRange["range"] = ranges[0]
    MasterService.keyRange["status"] = "True"
    lastport, _ = zk.get('/meta/lastport')
    lastport = int(lastport.decode('utf-8'))
    numOfServers = lastport - 8080 + 1
    MasterService.keyRange["backupPort"] = 8080 + ((8080 - portno) + 1) % numOfServers
    print("KEYRANGE: ", MasterService.keyRange["range"])
    print("ws://127.0.0.1:" + str(MasterService.keyRange["backupPort"]) + "/backup")
    ss = create_connection("ws://localhost:" + str(MasterService.keyRange["backupPort"]) + "/backup")

    signal = {
       "type": "setkey",
       "params": {"data": MasterService.keyRange["range"], "master": "true", "keyRanges": MasterService.keyRanges}
    }

    ss.send(json.dumps(signal))
    ss.close()
    print ("GOT ACK. ")

    time.sleep(6)

    scheduleSignals()

def clusterStatusUp():
    print(" ------------- CLUSTER STATUS UP ----------------- ")

def signalScheduler():
    s.enter(2, 1, scheduleMasterKeySet)
    s.run()
    #ss.run()

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
        print("init")
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
    keyRange = {"status": "false", "range": "", "backupPort": ""}
    test = 0

    def onMessage(self, payload, isBinary):
        if not isBinary:
            payload = json.loads(payload.decode('utf-8'))
            printout("[SLAVE]", YELLOW)
            print("RECIEVED: " + str(payload))
            payloadType = payload['type'].upper()
            payloadParams = payload['params']
            res = {
                'status': str(codes.SUCCESS.name)
            }

            if payloadType == 'SETKEY':
                printout("[SLAVE]", YELLOW)
                print("KEYSET RECIEVED FROM MASTER")
                self.keyRange["range"] = payloadParams['data']
                self.keyRange["status"] = "true"
                msg = json.dumps(res)
                printout("[SLAVE]", YELLOW)
                print("SENT KEYSET ACK. ")
                #self.proto.sendMessage(msg.encode('utf8'))


                lastport, _ = zk.get('/meta/lastport')
                lastport = int(lastport.decode('utf-8'))
                numOfServers = lastport - 8080 + 1
                print("PORT NUMBER", portno)
                self.keyRange["backupPort"] = 8080 + (abs(8080 - portno) + 1) % numOfServers
                printout("[SLAVE]", YELLOW)
                print("KEYRANGE: ", self.keyRange)

                #print("Sleeping for all clients to awake. ")
                #time.sleep(10)
                print("Finished Sleeping")
                print("ws://127.0.0.1:" + str(self.keyRange["backupPort"]) + "/backup")
                success = 0
                ss = None
                while(success == 0):
                    try:
                        ss = create_connection("ws://localhost:" + str(self.keyRange["backupPort"]) + "/backup")
                        success = 1
                    except Exception:
                        print("Server busy, trying again. ")
                        time.sleep(5)

                ss.send(json.dumps(payload))
                ss.close()
                print ("GOT ACK. ")

                global persistList
                persistList = zk.get_children('/app')
                #print("SETTING PERSISTLIST: ", persistList)

                return

            if self.keyRange["status"] == "false":
                res['status'] = str(codes.ERR_SERVER_NOT_INIT.name)

            if payloadType == 'GET':
                firstChar = ord(payloadParams['key'][0])
                start, end = self.keyRange['range'].split('-')
                start, end = int(start), int(end)
                if firstChar > start and firstChar <= end:
                    result = get(self.data, payloadParams['key'])
                    if result == codes.ERR_KEY_NOT_FOUND:
                        res['status'] = str(result.name)
                    else:
                        res['data'] = result
                else:
                    res['status'] = codes.ERR_KEY_NOT_RESPONSIBLE

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
                    ws = create_connection("ws://localhost:" + str(self.keyRange["backupPort"]) + "/backup")
                    ws.send(json.dumps(payload))
                    print ("Reeiving...")
                    result =  ws.recv()
                    print ("got ack. ")
                    ws.close()
                else:
                    res['status'] = codes.ERR_KEY_NOT_RESPONSIBLE

            elif payloadType == 'REPLICA':
                res['data'] = self.data


            else:
                res['status'] = str(codes.FAIL.name)
                res['data'] = "Operation not permitted"

            print(res)
            msg = json.dumps(res)
            print("SERVER SENT: " + msg)
            self.proto.sendMessage(msg.encode('utf8'))

class MasterService(BaseService):

    metadata = {}
    data = {}
    keyRange = {"status": "false", "range": "", "backupPort": ""}
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
                print(self.keyRange["range"])
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
                    ws = create_connection("ws://localhost:" + str(self.keyRange["backupPort"]) + "/backup")
                    ws.send(json.dumps(payload))
                    print ("Reeiving...")
                    result =  ws.recv()
                    print ("got ack. ")
                    ws.close()
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

    isMaster = {"flag": "false", "printString": ""}
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
            print("BACKUP RECEIVED: ", str(payload))
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
                    self.isMaster["printString"] = "[MASTER-BACKUP]"
                    printout("[MASTER-BACKUP]", MAGENTA)
                    print("ASSUMED MASTER BACKUP ROLE")
                    # check if backup of master
                    self.keyRanges = payloadParams["keyRanges"]
                    self.isMaster["flag"] = True
                else:
                    self.isMaster["printString"] = "[SLAVE-BACKUP]"
                    printout("[SLAVE-BACKUP]", MAGENTA)
                    print("ASSUMED SLAVE BACKUP ROLE")

                printout(self.isMaster["printString"], MAGENTA)
                print("BACKUP KEYRANGE: ", self.keyRange)
                printout(self.isMaster["printString"], MAGENTA)
                print("SENT ACK.")
                msg = json.dumps(res)
                if portno == 8080:
                    clusterStatusUp()
                #self.proto.sendMessage(msg.encode('utf8'))


                return

            if payloadType == 'GET':
                if self.isMaster["flag"]:
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
                else:
                    firstChar = ord(payloadParams['key'][0])
                    start, end = self.keyRange['range'].split('-')
                    start, end = int(start), int(end)
                    if firstChar > start and firstChar <= end:
                        result = put(self.data, payloadParams['key'], payloadParams['value'])
                        if result == codes.ERR_KEY_NOT_FOUND:
                            res['status'] = str(result.name)
                        else:
                            res['data'] = result
                    else:
                        res['status'] = codes.ERR_KEY_NOT_RESPONSIBLE

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
                if self.isMaster["flag"]:
                    keyRangeCheck = self.checkKeyRange(payloadParams['key'])
                    if keyRangeCheck == codes.SUCCESS:
                        # key in master
                        result = put(self.data, payloadParams['key'], payloadParams['value'])
                        if result == codes.ERR_KEY_ALREADY_EXISTS:
                            res['status'] = str(result.name)
                    else:
                        res["status"] = str(codes.ERR_KEY_NOT_RESPONSIBLE.name)
                        res["data"] = str(keyRangeCheck)
                else:
                    firstChar = ord(payloadParams['key'][0])
                    start, end = self.keyRange['range'].split('-')
                    start, end = int(start), int(end)
                    if firstChar > start and firstChar <= end:
                        result = put(self.data, payloadParams['key'], payloadParams['value'])
                    else:
                        res['status'] = codes.ERR_KEY_NOT_RESPONSIBLE

            elif payloadType == 'REPLICA':
                #res['data'] = self.data
                if self.isMaster["flag"]:
                    res['data'] = {
                        'keyRange': self.keyRange,
                        'keyRanges': self.keyRanges,
                        'data': self.data
                    }
                else:
                    res['data'] = {
                        'keyRange': self.keyRange,
                        'data': self.data
                    }
                res['status'] = str(codes.SUCCESS.name)


            else:
                res['status'] = str(codes.FAIL.name)
                res['data'] = "Operation not permitted"

            msg = json.dumps(res)
            print("SERVER SENT: " + msg)
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



import logging
logging.basicConfig()

currInstance = zk.create('/app/instance', ephemeral=True, sequence=True, makepath=True)
printout("[INIT]", WHITE)
print(currInstance, " started. ")

children = zk.get_children('/app')
printout("[INIT]", WHITE)
print(children)

portno = 0
portnum = 0
ranges = []

if len(children) == 1:
        portno = 8080
        printout("[MASTER]", RED)
        print("MASTER INIT")
        if zk.exists('/meta'):
            zk.delete('/meta', recursive=True)

        zk.create('/meta/master',b'8080', makepath=True)
        zk.create('/meta/status',b'initializing', makepath=True)
        zk.create('/meta/lastport','8080'.encode('utf-8'), makepath=True)
        time.sleep(5)
        children = zk.get_children('/app')
        config = {
            "mapper": {},
            "lastDead": {
                "backup": -1,
                # backup is the server port whose data the new server needs to retrieve
                "portno": -1
                # dead server portno
            },
            "numOfServers": len(children)
        }
        printout("[MASTER]", RED)
        children.sort()
        persistList = children
        print(children)
        counter = 0
        for child in children:
            config["mapper"][child] = 8080 + counter
            counter = counter + 1
        zk.create('/meta/config', json.dumps(config).encode('utf-8'))

        totalKeyRange = 122 - 48 + 1
        individualRange = int(math.floor(totalKeyRange / len(children)))
        start = 48
        for i in range(len(children)):
            ranges.append(str(start) + "-" + str(start+individualRange))
            start = start + individualRange + 1
        ranges[-1] = ranges[-1].split("-")[0] + "-122" # change this later please
        printout("[MASTER]", RED)
        print("RANGES: ", ranges)

        portnum, _ = zk.get('/meta/lastport')
        portnum = int(portnum.decode('utf-8'))


        signalScheduler()

else:
    try:
        config, _  = zk.get('/meta/config')
        config = json.loads(config.decode("utf-8"))
    except Exception as e:
        pass
    # if config["lastDead"]["portno"] != -1:
    #     # some server died
    #     portno = config["lastDead"]["portno"]
    #     masterDied = True if portno == 8080 else False
    #     portbackup = config["lastDead"]["backup"]
    #     print("REPLACEMENT SERVER FOR ", "[master]: " if masterDied else "[slave]: ",portno)
    #     if masterDied:
    #         config[currInstance] = portno # remap new instance
    #         payload = {
    #             type:"REPLICA"
    #         }
    #         print("ws://127.0.0.1:" + str(portbackup) + "/backup")
    #         ss = create_connection("ws://localhost:" + str(self.keyRange["backupPort"]) + "/backup")
    #         ss.send(json.dumps(payload))
    #         response = ss.recv()
    #         response = json.loads(response)
    #
    #         MasterService.keyRange = response["keyRange"]
    #         MasterService.keyRanges = response["keyRanges"]
    #         MasterService.data = response["data"]
    #
    #         ss.close()
    #
    #     else:
    #         pass
    #
    #     zk.set('/meta/config', json.dumps(config).encode('utf-8'))

    printout("[SLAVE]", YELLOW)
    print("SLAVE INIT")
    portno, _ = zk.get('/meta/lastport')
    portno = int(portno.decode('utf-8')) + 1
    zk.set('/meta/lastport', str(portno).encode())
    printout("[SLAVE]", YELLOW)
    print("LISTENING FOR KEYSET")
    print("Child watcher scheduled")
    scheduleChildWatcher()

# set port number to retrieve later
zk.set(currInstance, "{0}".format(str(portno)).encode('utf-8'))

factory = WebSocketServerFactory(u"ws://127.0.0.1:%s" % str(portno))
factory.protocol = ServiceServerProtocol
listenWS(factory)


reactor.run()
print("after")
