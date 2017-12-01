#!/usr/bin/env python

import csv
import sys
import time

#with open("/home/hduser/input1.txt", encoding="utf8") as file
    
url=None
#start_time = time.time()
for row in sys.stdin:
    list=row.split("\t")
  
   
    if(list[0]=='URL'):
        url=list[1]
    elif(list[0]=='TOKEN'):
           key=list[1]
        
           print('%s\t%s' % (key,url))
    for i in list:
        list.remove(i)

#print("Code Run Time for I/O operations  :   --- %s seconds ---" % (time.time() - start_time))
                




		
	
