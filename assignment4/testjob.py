from mrjob.job import MRJob
from mrjob.step import MRStep
import os
def getKey(x):
	return ord(x[0])

class MRWordFreqCount(MRJob):
	SORT_VALUES = True

	def steps(self):
		return [
			MRStep(mapper=self.mapper,reducer=self.reducer),
#			MRStep(reducer=self.identity_reducer)
		]

	def mapper(self, _, line):
		line = line.replace(" ","")
		line = line.split(",")
	
		if (line[1]!="0"): #matrix
			line[0],line[1]=line[1],line[0]
			yield line[1], "2m" + "," + line[0] + "," + line[2]
		else:
			yield line[0], "1n" + "," + "1" + "," + line[2]
			
	def reducer(self, key, values):
		vectorlist = next(values).split(',')
		for value in values:
			splitlist = values.split(',')
			yield (splitlist[1] + "," + vectorlist[1]),(int(splitlist[2])/length)*int(vectorlist[2])

	def identity_reducer(self, key, values):
		yield key, sum(values)
		
		
if __name__ == '__main__':
	MRWordFreqCount.run()
