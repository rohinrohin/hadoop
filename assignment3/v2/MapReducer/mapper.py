#!/usr/bin/env python
import sys
key=sys.argv[1]
entry=[]
for line in sys.stdin:
	if line == '':
		break
	if line == '\n':
		heads = [ i[0] for i in entry]
		tokens = entry[ heads.index('TOKEN'):]
		for i in tokens:
			if key==i[1]:
				print(entry[0][1]+'\t'+key)
				break
		sys.stdin.readline()
		entry=[]
		continue
	entry.append(line.split())

		