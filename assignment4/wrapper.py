#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWordFrequencyCount(MRJob):
    MRJob.SORT_VALUES = True
    pagerank = 0

    def init_mapper(self,_,line):
        x = line.split()
        url = x[0]
        pagerank = x[1]
        outlink_list = x[2:]    
        for outlink in outlink_list:
            yield outlink , float(pagerank) / len(outlink_list)    
    
        yield  url, outlink_list

    def mapper(self,key, value):
        url = key
        pagerank = value[0]
        outlink_list = value[1]    
        for outlink in outlink_list:
            yield outlink , float(pagerank) / len(outlink_list)    
    
        yield  url, outlink_list


    def reducer(self, url, pr_or_list):
        #print("Reducder : ", url , pr_or_list)
        for ele in pr_or_list:
            if isinstance(ele, list):
                outlink_list = ele
                yield(url, (self.pagerank, outlink_list))
                
                self.pagerank = 0
            else:


                self.pagerank += float(ele)


        self.pagerank = 0.15  + (0.85 * self.pagerank)


    def reducer_final(self, url, pr_or_list):
        #print("Reducder : ", url , pr_or_list)
        for ele in pr_or_list:
            if isinstance(ele, list):
                outlink_list = ele
                yield(self.pagerank, (url, outlink_list))
                
                self.pagerank = 0
            else:


                self.pagerank += float(ele)


        self.pagerank = 0.15  + (0.85 * self.pagerank)

    def steps(self):
        return [MRStep(mapper=self.init_mapper,reducer=self.reducer)]+[MRStep(mapper=self.mapper,reducer=self.reducer)]*8+[MRStep(mapper=self.mapper,reducer=self.reducer_final)]



if __name__ == '__main__':
    MRWordFrequencyCount().run()
