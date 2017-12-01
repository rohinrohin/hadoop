import sys

# for line in sys.stdin:
# 	print(1)


dict1={}
for line in sys.stdin:
	line.strip(' ')
	url,key = line.split('\t')
	if key in dict1.keys():
		dict1[key].append(url)
	else:
		dict1[key]=[url]
for i in dict1.keys():
	print(i+'\t'+str(dict1[i]))
