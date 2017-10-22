from kazoo.client import KazooClient 
import os 
import time

zk = KazooClient(hosts='127.0.0.1:2181') 
zk.start() 

import logging 
logging.basicConfig() 

currInstance = zk.create('/app/instance', ephemeral=True, sequence=True, makepath=True) 
print(currInstance, " started. ") 

children = zk.get_children('/app')

if len(children) == 1:
        zk.create('/app/config/master',b'8080', makepath=True)
        zk.create('/app/config/lastport',b'8080', makepath=True)
        zk.create('/app/config/status',b'initializing', makepath=T)
        time.sleep(20)
        children = zk.get_children('/app')
        if len(children) > 2:
                print('Server init')
                print(children)

persistList = list() 

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
