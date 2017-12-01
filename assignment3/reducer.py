#!/usr/bin/python
import sys
import json

count = 0
for line in sys.stdin:
    count += 1
        
print(count)