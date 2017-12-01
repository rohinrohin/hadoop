#!/usr/bin/python
import sys

import argparse
parser = argparse.ArgumentParser(description='Argument Parser') 
parser.add_argument('-k', '--keyword', required=False)
parser.add_argument('-c', '--count', required=False)
args = parser.parse_args()

if (args.keyword):
    keyword =   args.keyword
else:
    keyword = args.count  
    


def read_input(f):  
    entry = [] 
    for line in f:
        line =  line.strip()
        if 'URL' in line and '://' in line:
            url = line.split()[1]
        elif('TOKEN' in line and keyword == line.split()[1]):       
            entry.append(url)
    
    
    return entry

    

if __name__ == "__main__":
    # input comes from STDIN (standard input)	    
    
    data = read_input(sys.stdin)
    for words in data:
            print(words)
