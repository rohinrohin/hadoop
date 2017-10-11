from testjob import MRWordFreqCount
import sys
mr_job = MRWordFreqCount(args=['-r', 'local', 'matrix', 'vector'])
with mr_job.make_runner() as runner:
    runner.run()
    for line in runner.stream_output():
        key, value = mr_job.parse_output_line(line)
	print(key, value)

