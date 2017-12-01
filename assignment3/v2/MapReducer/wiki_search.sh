#!/bin/bash

if test $# -lt 1; then
	echo " wiki_search"
	echo "  -k <keyword> <file> { Map Reduce without a Combiner }"
	echo "  -c <keyword> <file> { Map Reduce with a Combiner }"
	echo "  -d { List available datasets in HDFS }"
fi

while test $# -gt 0; do
        case "$1" in
                -h|--help)
                        echo " wiki_search"
						echo "  -k <keyword> <file> { Map Reduce without a Combiner }"
						echo "  -c <keyword> <file> { Map Reduce with a Combiner }"
						echo "  -d { List available datasets in HDFS }"
						echo "  -h { Help }"
                        exit
                        ;;
                -d)
						/home/anvith/Hadoop/hadoop-2.8.1/bin/hdfs dfs -ls /Assignment1/Data
						exit
                        ;;
                -k)
						shift
						if test $# -gt 1; then
							/home/anvith/Hadoop/hadoop-2.8.1/bin/hdfs dfs -test -d /MapOutput1 && /home/anvith/Hadoop/hadoop-2.8.1/bin/hdfs dfs -rm -r -f /MapOutput1
	
				
							time /home/anvith/Hadoop/hadoop-2.8.1/bin/hadoop jar /home/anvith/Hadoop/hadoop-2.8.1/share/hadoop/tools/lib/hadoop-streaming-2.8.1.jar \
				            -mapper "python3 /home/anvith/BigData/MapReducer/mapper.py $1" \
				            -reducer "python3 /home/anvith/BigData/MapReducer/reducer.py" \
				            -input /Assignment1/Data/9 \
				            -output /MapOutput1
				            
						else
							echo "Ensure syntax : -k <keyword> <file>"

						fi
						exit
            			;;
		       	-c)
						shift
						if test $# -gt 1; then
							/home/anvith/Hadoop/hadoop-2.8.1/bin/hdfs dfs -test -d /MapOutput2 && /home/anvith/Hadoop/hadoop-2.8.1/bin/hdfs dfs -rm -r -f /MapOutput2


							time /home/anvith/Hadoop/hadoop-2.8.1/bin/hadoop jar /home/anvith/Hadoop/hadoop-2.8.1/share/hadoop/tools/lib/hadoop-streaming-2.8.1.jar \
				            -D  mapreduce.job.reduces=2 \
				            -mapper "python3 /home/anvith/BigData/MapReducer/mapper.py $1" \
				            -combiner "python3 /home/anvith/BigData/MapReducer/combiner.py" \
				            -reducer "python3 /home/anvith/BigData/MapReducer/reducer1.py" \
				            -input /Assignment1/Data/9 \
				            -output /MapOutput2

						else
							echo "Ensure syntax : -c <keyword> <file>"

						fi
						exit
            			;;
				*)
					echo " wiki_search --Big Data Assignment"
					echo "  -k <keyword> <file> { Map Reduce without a Combiner }"
					echo "  -c <keyword> <file> { Map Reduce with a Combiner }"
					echo "  -d { List available datasets in HDFS }"
					exit
					;;
	esac
done														
