from mrjob.job import MRJob
from mrjob.step import MRStep
import os

class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        try:
            input_file = os.environ['mapreduce_map_input_file']
        except KeyError:
            input_file = os.environ['map_input_file']
            
        line = line.split()
        if (input_file == "matrix"): #matrix
            yield (line[1]), ("m" + "," + line[0] + "," + line[2])
        else:
            yield (line[0]), ("n" + "," + line[1] + "," + line[2])
            
    def reducer(self, key, value):
        yield key, value 
        
if __name__ == '__main__':
    MRWordFreqCount.run()
