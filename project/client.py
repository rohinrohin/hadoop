from websocket import create_connection
import json
import sys
import ast
from enum import Enum

MASTER = "8080"

from collections import OrderedDict

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
<<<<<<< 17dcf3916e4c634834cb7464dc4429e3d62a14ef
	SUCCESS = 1
	FAIL = 2
	ERR_KEY_ALREADY_EXISTS = 3
	ERR_KEY_NOT_RESPONSIBLE = 4
	ERR_KEY_NOT_FOUND = 5
    ERR_SERVER_NOT_INIT = 6
=======
    SUCCESS = 1
    FAIL = 2
    ERR_KEY_ALREADY_EXISTS = 3
    ERR_KEY_NOT_RESPONSIBLE = 4
    ERR_KEY_NOT_FOUND = 5
>>>>>>> added terminal colors

def connect_to_server(request, port_num, isMaster):
    address_append = "/keystore" if not isMaster else "/master"
    address="ws://127.0.0.1:"+port_num+address_append
    print("Address: " + address)
    ws=create_connection(address)
<<<<<<< 17dcf3916e4c634834cb7464dc4429e3d62a14ef
    if request['type'] == "get":
        if isMaster:
            listCacheKeys=list(client_cache.keys())
            for i in listCacheKeys:
                if i == request['key']:
                    print("Port associated with key exists in cache")
                    print("Cache returned port number:", client_cache[i])
                    connect_to_server(request, str(client_cache[i]), isMaster=False)
    elif request['type'] == "put":
        if request['value'][0] == '{':
            request['value'] = literal_eval(request['value'])
=======
    request = input().split()
    #if len(request) > 3:
        # hack to allow spaces in json value input
    #   request = [request[0],request[1],''.join(request[2:])]
    print(request[0])
    print(request[1])
    #print(request[2])
    method, params = request[0].lower(), ""
    if method == "get":
        listCacheKeys=list(client_cache.keys())
        for i in listCacheKeys:
            if i==(request[1]):
                print("recieved locally without master")
                #value=client_cache[]
                print("cache returned:", client_cache[i])
                return connect_to_server(str(client_cache[i]), isMaster=False)
        params = {
            "key": request[1]
        }
    elif method == "put":
        value = ''.join(request[2:])
        if value[0] == '{':
            value = literal_eval(value)
>>>>>>> added terminal colors
        else:
            request['value'] = request['value'].strip()
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
    ws.send(json.dumps(message).encode('utf8'))
    print("Sent")
    print("Receiving...")
    result =  ws.recv()
    result = json.loads(result)
    print ("Server returned ",result["status"])
<<<<<<< 17dcf3916e4c634834cb7464dc4429e3d62a14ef
    if(result['status'] == codes.SUCCESS.name):
        client_cache[request['key']] = port_num
=======
    #print(client_cache[request[1]])

    if(result['status']==codes.SUCCESS.name):
        client_cache[request[1]]=port_num
>>>>>>> added terminal colors
        print(client_cache)  #if the master contains the key. 9001 is the current static master address

    if(result['status'] == codes.ERR_KEY_NOT_RESPONSIBLE.name):
        if "data" in result:
            print("Contact port number "+result["data"])
            new_port_no = result['data']
            connect_to_server(request, str(new_port_no), isMaster=False)
            # MASTER SENT NEW PORT NUMBER
        else:
            # CACHED SLAVE NO LONGER EXISTS/HOLDS KEY
            # SO CONTACT MASTER AGAIN
            # PARTHA COME CALL ME WHEN YOU ARE SEEING THIS CODE
            connect_to_server(request, MASTER, isMaster=True)

    ws.close()

def send_request(request):
    request = request.split()
    request = {
        'type': request[0].lower(),
        'key': request[1],
        'value': ''.join(request[2:]) if len(request) >= 3 else ''
    }
    print("Client's request: ",request)
    while(True):
        connect_to_server(request, MASTER, isMaster=True)

if __name__ == '__main__':
    send_request(input())
