from kazoo.client import KazooClient 
import os 

zk = KazooClient(hosts='127.0.0.1:2181') 
zk.start() 

import logging 
logging.basicConfig() 

currInstance = zk.create('/app', ephemeral=True, sequence=True, makepath=True) 
print(currInstance, " started. ") 

persistList = list() 

@zk.ChildrenWatch("/") 
def watch_children(children): 
       global persistList 

       nodelist = [] 
       for child in children: 
               if 'app' in child: 
                       nodelist.append(int(child[-10:])) 
       print("nodelist: ", nodelist) 
       nodelist.sort(reverse=True) 

       if (len(persistList) > len(nodelist)): 
               print("process died!") 

               if (nodelist[0] == int(currInstance[-10:])): 
                       print("LEADER:", currInstance," starting new process") 
                       os.system("python3 zoo.py") 

       persistList = nodelist.copy() 

while True: 
       pass
