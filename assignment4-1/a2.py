#!/usr/bin/env python

from operator import itemgetter
import sys
import time
from datetime import timedelta

current_key = None
current_url = None
url = None
count = 0
key='chronic'
start_time = time.time()
# input comes from STDIN
for line in sys.stdin:
    #print(type(line))
    #print(line.split(' '))
    line=line.strip()
    #key,url=
    if(line!= ""):
       arr=line.split('\t',1)
       if(arr[0]!=' '):
          
          current_key=arr[0]
          url=arr[1]
          if(current_key == key):
	      count=count+1
              print(key,url)
print("total relevant links found" , count)	      
print("Code Run Time   :   --- %s seconds ---" % (time.time() - start_time))
