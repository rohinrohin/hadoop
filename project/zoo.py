from kazoo.client import KazooClient 
import os 
import time
import json

zk = KazooClient(hosts='192.168.31.233:2181') 
zk.start() 

import logging 
logging.basicConfig() 

currInstance = zk.create('/app/instance', ephemeral=True, sequence=True, makepath=True) 
print(currInstance, " started. ") 

children = zk.get_children('/app')
print(children)

if len(children) == 1:
        print("SERVER")
        if zk.exists('/meta'):
            zk.delete('/meta', recursive=True)

        zk.create('/meta/master',b'8080', makepath=True)
        zk.create('/meta/status',b'initializing', makepath=True)
        zk.create('/meta/lastport',b'8080', makepath=True)
        time.sleep(20)
        children = zk.get_children('/app')
        config = {
            "lastDead": -1, 
            "numOfServers": len(children)
        }
        zk.create('/meta/config', json.dumps(config).encode('utf-8'))
        print('Server init')
        print(children)

#persistList = list() 
#@zk.ChildrenWatch("/") 
#def watch_children(children): 
       #global persistList 
#
       #nodelist = [] 
       #for child in children: 
               #if 'app' in child: 
                       #nodelist.append(int(child[-10:])) 
       #print("nodelist: ", nodelist) 
       #nodelist.sort(reverse=True) 
#
       #if (len(persistList) > len(nodelist)): 
               #print("process died!") 
#
               #if (nodelist[0] == int(currInstance[-10:])): 
                       #print("LEADER:", currInstance," starting new process") 
                       #os.system("python3 zoo.py") 
#
       #persistList = nodelist.copy() 

while True: 
       pass
