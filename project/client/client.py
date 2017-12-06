from kazoo.client import KazooClient
from websocket import create_connection
import json
import sys
from ast import literal_eval
from enum import Enum
from collections import OrderedDict
import copy

MASTER = "8080"
LOG = []
CLUSTER_STATUS = ""

def logger(log):
    LOG.append(log)

def clean_log():
    global LOG
    LOG = []

zk = KazooClient(hosts='192.168.31.233:2181')

def start_zookeeper():
    zk.start()
    @zk.DataWatch('/meta/master')
    def get_master(data, stat):
        global MASTER
        MASTER = data.decode("utf-8")
        logger("Current MASTER port: "+MASTER)

    @zk.DataWatch('/meta/status')
    def cluster_status(data, stat):
        global CLUSTER_STATUS
        CLUSTER_STATUS = data.decode("utf-8")
        logger("CLUSTER STATUS: "+CLUSTER_STATUS)

class LimitedSizeDict(OrderedDict):
  def __init__(self, *args, **kwds):
    self.size_limit = kwds.pop("size_limit", None)
    OrderedDict.__init__(self, *args, **kwds)
    self._check_size_limit()

  def __setitem__(self, key, value):
    OrderedDict.__setitem__(self, key, value)
    self._check_size_limit()

  def _check_size_limit(self):
    if self.size_limit is not None:
      while len(self) > self.size_limit:
        self.popitem(last=False)

client_cache= LimitedSizeDict(size_limit=10)


class codes(Enum):
	SUCCESS = 1
	FAIL = 2
	ERR_KEY_ALREADY_EXISTS = 3
	ERR_KEY_NOT_RESPONSIBLE = 4
	ERR_KEY_NOT_FOUND = 5
    #ERR_SERVER_NOT_INIT = 6

def connect_to_server(request, port_num, isMaster, useCache=True):
    address_append = "/keystore" if not isMaster else "/master"
    # if a server has died and the backup port is returned back
    if '/backup' in port_num:
        address_append = ''
    address="ws://127.0.0.1:"+port_num+address_append
    #logger(address)
    if request['type'] == "get":
        if isMaster and useCache:
            listCacheKeys=list(client_cache.keys())
            cacheMiss = True
            for i in listCacheKeys:
                if i == request['key']:
                    cacheMiss = False
                    logger("CACHE HIT, port returned: "+client_cache[i])
                    if str(client_cache[i]) != MASTER:
                        return connect_to_server(request, str(client_cache[i]), isMaster=False)
            if cacheMiss:
                logger('CACHE MISS')
    #elif request['type'] == "put":
        #if request['value'][0] == '{':
        #request['value'] = literal_eval(request['value'])
        #else:
        #    request['value'] = request['value'].strip()
    #elif method == "getmultiple":
    #    params = {
    #        "keys": request[1:]
    #    }
    message = {
        "type": request['type'],
        "params": {
            'key': request['key'],
            'value': request['value']
        }
    }
    logger('Establishing connection with '+address)
    ws = None
    try:
        ws = create_connection(address)
    except Exception:
        logger("CONNECTION FAILED!")
        return connect_to_server(request, MASTER, isMaster=True, useCache=False)
    ws.send(json.dumps(message).encode('utf8'))
    result =  ws.recv()
    result = json.loads(result)
    logger(result["status"])
    ws.close()
    if(result['status'] == codes.SUCCESS.name):
        if "/backup" not in port_num:
            # backup in port_num so don't cache since server might come up
            client_cache[request['key']] = port_num
        if 'data' in result:
            logger('Data returned: '+result['data'])
    elif(result['status'] == codes.ERR_KEY_NOT_RESPONSIBLE.name):
        if "data" in result:
            logger("Contacting port number "+result["data"])
            new_port_no = result['data']
            return connect_to_server(request, str(new_port_no), isMaster=False)
            # MASTER SENT NEW PORT NUMBER
        else:
            # CACHED SLAVE NO LONGER EXISTS/HOLDS KEY
            # SO CONTACT MASTER AGAIN
            # PARTHA COME CALL ME WHEN YOU ARE SEEING THIS CODE
            return connect_to_server(request, MASTER, isMaster=True)

    logger("Current Cache:" + json.dumps(client_cache))  #if the master contains the key. 9001 is the current static master address
    LOG_COPY = copy.deepcopy(LOG)
    result = {
        'status': result['status'],
        'data': result['data'] if 'data' in result else '',
        'logger': LOG_COPY
    }
    clean_log()
    return result

def send_request(request):
    request = request.split()
    request = {
        'type': request[0].lower(),
        'key': request[1],
        'value': ''.join(request[2:]) if len(request) >= 3 else ''
    }
    start_zookeeper()
    print("Client's request: ",request)
    response = connect_to_server(request, MASTER, isMaster=True)
    print(response)
    clean_log()

def send_request_json(request):
    logger(" ---------------- " + request['type'].upper())
    request = {
        'type': request['type'],
        'key': request['key'],
        'value': request['value'] if 'value' in request else ''
    }
    start_zookeeper()
    logger("Client Request: " + json.dumps(request))
    result = connect_to_server(request, MASTER, isMaster=True)
    logger(" ---------------")

    return result

def get_cluster_status():
    return CLUSTER_STATUS

if __name__ == '__main__':

    while True:
        send_request(input())
