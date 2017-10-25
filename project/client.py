from websocket import create_connection
import json
import sys
import ast
from enum import Enum

MASTER = "9001"

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
	SUCCESS = 1
	FAIL = 2
	ERR_KEY_ALREADY_EXISTS = 3
	ERR_KEY_NOT_RESPONSIBLE = 4
	ERR_KEY_NOT_FOUND = 5 

def connect_to_server(port_num):
    address="ws://127.0.0.1:"+port_num+"/keystore"
    ws=create_connection(address)
    request = input().split()
    #if len(request) > 3:
        # hack to allow spaces in json value input
    #	request = [request[0],request[1],''.join(request[2:])]
    print(request[0])
    print(request[1])
    #print(request[2])
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
    #self.sendMessage(json.dumps(message).encode('utf8'))
    #print("Sending 'Hello, World'...")
    ws.send(json.dumps(message).encode('utf8'))
    print("Sent")
    print("Receiving...")
    result =  ws.recv()
    result = json.loads(result)
    print(request[1])
    print("999999")
    print ("Server returned ",result["status"])
    #print(client_cache[request[1]])
    if(method=='get'):
        listCacheKeys=list(client_cache.keys())
        for i in listCacheKeys:
            if i==(request[1]):
                print("recieved locally without master")
                #value=client_cache[]
                print("cache returned:",)
        print ("Server returned data: ",result["data"])

    if(result['status']==codes.SUCCESS.name):
        client_cache[request[1]]=port_num
        


        print(client_cache)  #if the master contains the key. 9001 is the current static master address	
    if(result['status'] == codes.ERR_KEY_NOT_RESPONSIBLE.name ):
        print("Contact port number "+result["data"])
        #ws.close()        
        new_port_no = result['data']
        connect_to_server(str(new_port_no))    
    ws.close()

while(True):
    connect_to_server(MASTER)
    
    


    #print("Received '%s'" % result)
    #print("result.status" result.status)
   
