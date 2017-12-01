import sys
count=0
for line in sys.stdin:
	urls = line.split('\t')[1]
	count += len(urls.split(','))

print(count)