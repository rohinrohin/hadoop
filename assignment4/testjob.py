from mrjob.job import MRJob
from mrjob.step import MRStep
import os
def getKey(x):
	return ord(x[0])

class MRWordFreqCount(MRJob):

	def steps(self):
		return [
			MRStep(mapper=self.mapper,reducer=self.reducer),
			MRStep(reducer=self.identity_reducer)

		]

	def mapper(self, _, line):
		line = line.replace(" ","")
		line = line.split(",")
	
		if (line[1]!="0"): #matrix
			line[0],line[1]=line[1],line[0]
			yield line[1], "m" + "," + line[0] + "," + line[2]
		else:
			yield line[0], "n" + "," + "1" + "," + line[2]
			
	def reducer(self, key, values):
		sortList = sorted(values, key=getKey, reverse=True)
		length = len(sortList) - 1
		vectorlist = sortList[0].split(',')
		for i in range(1, length+1):
			splitlist = sortList[i].split(',')
			yield (splitlist[1] + "," + vectorlist[1]),(int(splitlist[2])/length)*int(vectorlist[2])

	def identity_reducer(self, key, values):
		yield key, sum(values)
		
		
if __name__ == '__main__':
	MRWordFreqCount.run()
