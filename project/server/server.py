import txaio
import copy
import traceback
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

zk = KazooClient(hosts='127.0.0.1:2181')
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
            zk.set('/meta/status', 'UNSTABLE'.encode('utf-8'))
            config, _ = zk.get("/meta/config")
            config = json.loads(config.decode("utf-8"))
            #print(deadSet)
            deadInstance = str(list(deadSet)[0])
            deadInstancePort = config["mapper"][deadInstance]
            deadInstanceBackup = 8080 + ((abs(deadInstancePort-8080) +1) % config["numOfServers"])

            if deadInstancePort == 8080:
                # master died
                    print("NOTIFICATION:", deadSet, " [master] died. ")
                    if portno == deadInstanceBackup:
                        printout("[MASTER-BACKUP]", MAGENTA)
                        print("MASTER DIED, MASTER'S BACKUP DOING HANDOVER. ")
                        # master's backup slave
                        zk.set('/meta/master', (str(deadInstanceBackup)+'/backup').encode('utf-8')) # setting new master
                        printout("[MASTER-BACKUP]", MAGENTA)
                        print("SLAVE PERSISTING DEAD MASTER CONFIG")
                        config["lastDead"]["portno"] = deadInstancePort
                        config["lastDead"]["backup"] = deadInstanceBackup
                        printout("[MASTER-BACKUP]", MAGENTA)
                        print("HANDOVER COMPLETE.  ")
                        printout("[MASTER-BACKUP]", MAGENTA)
                        print("SETTING CONFIG: ", config)
                        zk.set('/meta/config', json.dumps(config).encode('utf-8'))
            else:
                # slave died
                printout("[SLAVE-BACKUP]", MAGENTA)
                print("NOTIFICATION:", deadSet, " [slave] died. ")
                masterPort, _ = zk.get('meta/master')
                masterPort = masterPort.decode("utf-8")
                if portno == int(masterPort):
                    #inside master
                    printout("[SLAVE-BACKUP]", MAGENTA)
                    print("MASTER PERSISTING DEAD SLAVE CONFIG")
                    config["lastDead"]["portno"] = deadInstancePort
                    config["lastDead"]["backup"] = deadInstanceBackup
                    # set keyrange for backup server
                    for key, value in MasterService.keyRanges.items():
                        if value == deadInstancePort:
                            MasterService.keyRanges[key] = str(deadInstanceBackup) + "/backup"

                    printout("[SLAVE-BACKUP]", MAGENTA)
                    print("SETTING CONFIG: ", config)
                    zk.set('/meta/config', json.dumps(config).encode('utf-8'))

            del config["mapper"][deadInstance] # remove now reduntant instance
        persistList = nodelist.copy()


def scheduleSignals(a='default'):
    printout("[MASTER]", RED)
    print("START SIGNAL SCHEDULED")
    printout("[MASTER]", RED)
    print("CHILD WATCHER SCHEDULED")
    scheduleChildWatcher()
    if portno == 8080:
        for port in range(8081, portnum + 1):

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
    printout("[MASTER]", RED)
    print("MASTER BACKUP KEYSET SCHEDULED")
    MasterService.keyRange["range"] = ranges[0]
    MasterService.keyRange["status"] = "True"
    lastport, _ = zk.get('/meta/lastport')
    lastport = int(lastport.decode('utf-8'))
    numOfServers = lastport - 8080 + 1
    MasterService.keyRange["backupPort"] = 8080 + ((8080 - portno) + 1) % numOfServers
    printout("[MASTER]", RED)
    print("KEYRANGE: ", MasterService.keyRange["range"])
    printout("[MASTER]", RED)
    print("ws://127.0.0.1:" + str(MasterService.keyRange["backupPort"]) + "/backup")
    ss = create_connection("ws://localhost:" + str(MasterService.keyRange["backupPort"]) + "/backup")

    signal = {
       "type": "setkey",
       "params": {"data": MasterService.keyRange["range"], "master": "true", "keyRanges": MasterService.keyRanges, "backupPort": MasterService.keyRange["backupPort"]}
    }

    ss.send(json.dumps(signal))
    ss.close()
    printout("[MASTER]", RED)
    print ("GOT ACK. ")

    time.sleep(6)

    scheduleSignals()

def clusterStatusUp():
    printout("[MASTER]", RED)
    print(" -------------------- CLUSTER STATUS UP --------------------")
    zk.set('/meta/status', 'STABLE'.encode('utf-8'))

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
    keyRange = {"status": "false", "range": "", "backupPort": ""}

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
                printout("[SLAVE]", YELLOW)
                print("PORT NUMBER", portno)
                self.keyRange["backupPort"] = 8080 + (abs(8080 - portno) + 1) % numOfServers
                printout("[SLAVE]", YELLOW)
                print("KEYRANGE: ", self.keyRange)

                #print("Sleeping for all clients to awake. ")
                #time.sleep(10)
                payload["params"]["backupPort"] = self.keyRange["backupPort"]
                printout("[SLAVE]", YELLOW)
                print("Finished Sleeping")
                printout("[SLAVE]", YELLOW)
                print("ws://127.0.0.1:" + str(self.keyRange["backupPort"]) + "/backup")
                success = 0
                ss = None
                while(success == 0):
                    try:
                        ss = create_connection("ws://localhost:" + str(self.keyRange["backupPort"]) + "/backup")
                        success = 1
                    except Exception:
                        printout("[SLAVE]", YELLOW)
                        print("Server busy, trying again. ")
                        time.sleep(5)

                ss.send(json.dumps(payload))
                ss.close()
                printout("[SLAVE]", YELLOW)
                print ("GOT ACK. ")

                global persistList
                persistList = zk.get_children('/app')
                #print("SETTING PERSISTLIST: ", persistList)

                return

            if self.keyRange["status"] == "false":
                res['status'] = str(codes.ERR_SERVER_NOT_INIT.name)

            if payloadType == 'REPLICA':
                #res['data'] = self.data
                res['data'] = {
                    'keyRange': self.keyRange,
                    'data': self.data
                }
                res['status'] = str(codes.SUCCESS.name)

            elif payloadType == 'GET':
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
                    try:
                        ws = create_connection("ws://localhost:" + str(self.keyRange["backupPort"]) + "/backup")
                        ws.send(json.dumps(payload))
                        printout("[SLAVE]", YELLOW)
                        print ("Receiving...")
                        result =  ws.recv()
                        printout("[SLAVE]", YELLOW)
                        print ("Got ACK")
                        ws.close()
                    except Exception as e:
                        printout("[SLAVE]", YELLOW)
                        print("UNABLE TO CONTACT BACKUP SERVER. SERVER MIGHT POSSIBLY BE DOWN ")
                else:
                    res['status'] = codes.ERR_KEY_NOT_RESPONSIBLE

            elif payloadType == 'REPLICA':
                res['data'] = self.data


            else:
                res['status'] = str(codes.FAIL.name)
                res['data'] = "Operation not permitted"

            msg = json.dumps(res)
            printout("[SLAVE]", YELLOW)
            print("SERVER SENT: " + msg)
            self.proto.sendMessage(msg.encode('utf8'))

class MasterService(BaseService):

    metadata = {}
    data = {}
    keyRange = {"status": "false", "range": "", "backupPort": ""}
    keyRanges = {"ranges": {}}


    def checkKeyRange(self, key):
        start, end = self.keyRange['range'].split('-')
        start, end = int(start), int(end)
        firstChar = ord(key[0])
        if not (firstChar > start and firstChar <= end):
            for key in self.keyRanges["ranges"]:
                start, end = key.split('-')
                start, end = int(start), int(end)
                if firstChar > start and firstChar <= end:
                    return self.keyRanges["ranges"][key]

        return (codes.SUCCESS)

    def onMessage(self, payload, isBinary):
        if not isBinary:
            payload = json.loads(payload.decode('utf-8'))
            printout("[MASTER]", RED)
            print("MASTER SERVER RECEIVED: " + str(payload))
            payloadType = payload['type'].upper()
            payloadParams = payload['params']
            res = {
                'status': str(codes.SUCCESS.name)
            }

            if self.keyRange["status"] == "false":
                res['status'] = str(codes.ERR_SERVER_NOT_INIT.name)

            if payloadType == 'REINCARNATE':
                printout("[MASTER]", RED)
                print("REINCARNATION REQUEST RECIEVED")
                deadInstancePort = payloadParams["deadPort"]
                deadInstanceBackup = payloadParams["backupPort"]
                print("HANDING OVER ", str(deadInstanceBackup) + "/backup", "TO ", deadInstancePort)
                for key, value in self.keyRanges["ranges"].items():
                    if value == str(deadInstanceBackup)+"/backup":
                        self.keyRanges["ranges"][key] = deadInstancePort

                printout("[MASTER]", RED)
                print("REINCARNATION SUCCESSFUL")

            elif payloadType == 'REPLICA':
                #res['data'] = self.data
                res['data'] = {
                    'keyRange': self.keyRange,
                    'keyRanges': self.keyRanges,
                    'data': self.data
                }
                res['status'] = str(codes.SUCCESS.name)

            elif payloadType == 'GET':
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
                    try:
                        ws = create_connection("ws://localhost:" + str(self.keyRange["backupPort"]) + "/backup")
                        ws.send(json.dumps(payload))
                        printout("[MASTER]", RED)
                        print ("Receiving...")
                        result =  ws.recv()
                        printout("[MASTER]", RED)
                        print ("Got ACK")
                        ws.close()
                    except Exception as e:
                        printout("[MASTER]", RED)
                        print("UNABLE TO CONTACT BACKUP SERVER. SERVER MIGHT POSSIBLY BE DOWN")
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
            printout("[MASTER]", RED)
            print("SERVER SENT: " + msg)
            self.proto.sendMessage(msg.encode('utf8'))

class BackupKeyStoreService(BaseService):

    isMaster = {"flag": "false", "printString": ""}
    data = {}
    keyRange = {"status": "false", "range": "", "backupPort": ""}
    keyRanges = {"ranges": {}}

    def checkKeyRange(self, key):
        start, end = self.keyRange['range'].split('-')
        start, end = int(start), int(end)
        firstChar = ord(key[0])
        if not (firstChar > start and firstChar <= end):
            for key in self.keyRanges["ranges"]:
                start, end = key.split('-')
                start, end = int(start), int(end)
                if firstChar > start and firstChar <= end:
                    return self.keyRanges["ranges"][key]

        return (codes.SUCCESS)

    def onMessage(self, payload, isBinary):
        if not isBinary:

            payload = json.loads(payload.decode('utf-8'))
            payloadType = payload['type'].upper()
            payloadParams = payload['params']
            printStr = "[MASTER-BACKUP]" if "master" in payloadParams else "[SLAVE-BACKUP]"
            printout(printStr, MAGENTA)
            print("BACKUP RECEIVED: ", str(payload))
            res = {
                'status': str(codes.SUCCESS.name)
            }

            if payloadType == 'SETKEY':
                printout(printStr, MAGENTA)
                print("KEYSET RECIEVED")
                self.keyRange["range"] = payloadParams['data']
                self.keyRange["status"] = "true"
                self.keyRange["backupPort"] = payloadParams["backupPort"]

                if "master" in payloadParams:
                    self.isMaster["printString"] = "[MASTER-BACKUP]"
                    printout("[MASTER-BACKUP]", MAGENTA)
                    print("ASSUMED MASTER BACKUP ROLE")
                    # check if backup of master
                    self.keyRanges["ranges"] = payloadParams["keyRanges"]
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
            printout(printStr, MAGENTA)
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
serverDied = False

if len(children) == 1:
        portno = 8080
        printout("[MASTER]", RED)
        print("MASTER INIT")
        if zk.exists('/meta'):
            zk.delete('/meta', recursive=True)

        zk.create('/meta/master',b'8080', makepath=True)
        zk.create('/meta/status','INIT'.encode("utf-8"), makepath=True)
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

        portnum, _ = zk.get('/meta/lastport')
        portnum = int(portnum.decode("utf-8"))

        MasterService.keyRanges["ranges"][ranges[0]] = 8080
        for port in range(8081, portnum + 1):
            MasterService.keyRanges["ranges"][ranges[port - 8080]] = port

        printout("[MASTER]", RED)
        print("RANGES: ", ranges)

        portnum, _ = zk.get('/meta/lastport')
        portnum = int(portnum.decode('utf-8'))


        signalScheduler()

else:
    try:
        config, _  = zk.get('/meta/config')
        # dead server detected
        # REINCARNATION MODE
        config = json.loads(config.decode("utf-8"))

        if config["lastDead"]["portno"] != -1:
            serverDied = True
            portno = config["lastDead"]["portno"]
            masterDied = True if portno == 8080 else False
            portbackup = config["lastDead"]["backup"]
            printStr = "[REINCARNATION]"
            colorr = CYAN
            printout(printStr, colorr)
            print("REINCARNATION SERVER BOOTING FOR ", "[master]: " if masterDied else "[slave]: ", portno)

            # get it servers own data from the next server/backup
            payload = {
                "type":"REPLICA",
                "params": ""
            }
            printout(printStr, colorr)
            print("ws://127.0.0.1:" + str(portbackup) + "/backup")
            ss = create_connection("ws://localhost:" + str(portbackup) + "/backup")
            ss.send(json.dumps(payload))
            response = ss.recv()
            ss.close()
            response = json.loads(response)

            payload = {
                "type":"REPLICA",
                "params": ""
            }

            # get its /backup contents from previous servers/keystore or /master
            backupContentPort = (8080 + config["numOfServers"] - 1) if (abs(portno-8080)-1) < 0 else portno-1
            backupContentPortAppend = "/master" if backupContentPort == 8080 else "/keystore"
            printout(printStr, colorr)
            print("ws://localhost:" + str(backupContentPort) + str(backupContentPortAppend))
            ss = create_connection("ws://localhost:" + str(backupContentPort) + str(backupContentPortAppend))
            ss.send(json.dumps(payload))
            backupResponse= ss.recv()
            ss.close()
            backupResponse = json.loads(backupResponse)


            new_config = copy.deepcopy(config)

            new_config["mapper"][currInstance.split("/")[2]] = portno # remap new instance
            new_config["lastDead"] = {
                "backup": -1,
                "portno": -1
            }
            zk.set('/meta/config', json.dumps(new_config).encode('utf-8'))
            zk.set("/meta/status", "STABLE".encode("utf-8"))
            if masterDied:

                MasterService.keyRange = response["data"]["keyRange"]
                MasterService.keyRanges = response["data"]["keyRanges"]
                MasterService.data = response["data"]["data"]

                BackupKeyStoreService.keyRange = backupResponse["data"]["keyRange"]
                BackupKeyStoreService.data = backupResponse["data"]

                printout("[REINCARNATION]", RED)
                print ("MASTER REINCARNATING ITSELF")
                zk.set('/meta/master', str(portno).encode('utf-8'))
                printout("[REINCARNATION]", RED)
                print ("MASTER KEY HANDOVER DONE")
                scheduleChildWatcher()

                printout("[MASTER]", RED)
                print ("CLUSTER IS NOW STABLE.")




            else:

                BackupKeyStoreService.keyRange = backupResponse["data"]["keyRange"]
                BackupKeyStoreService.data = backupResponse["data"]["data"]
                if backupContentPort == 8080:
                    # masters backup is in /backup
                    BackupKeyStoreService.keyRanges = backupResponse["data"]["keyRanges"]


                KeyStoreService.keyRange = response["data"]["keyRange"]
                KeyStoreService.data = response["data"]["data"]

                payload = {
                    "type": "REINCARNATE",
                    "params": {
                        "deadPort": config["lastDead"]["portno"],
                        "backupPort": config["lastDead"]["backup"]
                    }
                }

                masterPort, _ = zk.get("/meta/master")
                masterPort = masterPort.decode("utf-8")
                ss = create_connection("ws://localhost:" + masterPort + "/master")
                ss.send(json.dumps(payload))
                #response = ss.recv()
                ss.close()
                scheduleChildWatcher()
                printout("[SLAVE]", YELLOW)
                print("SERVER BOOTED")


    except Exception as e:
        pass

    if not serverDied:
        printout("[SLAVE]", YELLOW)
        print("SLAVE INIT")
        portno, _ = zk.get('/meta/lastport')
        portno = int(portno.decode('utf-8')) + 1
        zk.set('/meta/lastport', str(portno).encode())
        printout("[SLAVE]", YELLOW)
        print("LISTENING FOR KEYSET")
        printout("[SLAVE]", YELLOW)
        print("Child watcher scheduled")
        scheduleChildWatcher()

# set port number to retrieve later
zk.set(currInstance, "{0}".format(str(portno)).encode('utf-8'))

factory = WebSocketServerFactory(u"ws://127.0.0.1:%s" % str(portno))
factory.protocol = ServiceServerProtocol
listenWS(factory)

reactor.run()
