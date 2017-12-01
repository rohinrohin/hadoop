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
						hdfs dfs -ls /Assignment1/Data
						exit
                        ;;
                -k)
						shift
						if test $# -gt 1; then
							hdfs dfs -test -d /MapOutput1 && hdfs dfs -rm -r -f /MapOutput1
	
				
							time /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.6.5.jar \
				            -mapper "python3 /home/hduser/hadoop/assignment3/v2/mapper.py $1" \
				            -reducer "python3 /home/hduser/hadoop/assignment3/v2/reducer.py" \
				            -input /Assignment/3 \
				            -output /MapOutput1
				            
						else
							echo "Ensure syntax : -k <keyword> <file>"

						fi
						exit
            			;;
		       	-c)
						shift
						if test $# -gt 1; then
							/usr/local/hadoop/bin/hdfs dfs -test -d /MapOutput2 && /usr/local/hadoop/bin/hdfs dfs -rm -r -f /MapOutput2


							time /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.6.5.jar \
				            -D  mapreduce.job.reduces=2 \
				            -mapper "python3 /home/hduser/hadoop/assignment3/v2/mapper.py $1" \
				            -combiner "python3 /home/hduser/hadoop/assignment3/v2/combiner.py" \
				            -reducer "python3 /home/hduser/hadoop/assignment3/v2/reducer1.py" \
				            -input /Assignment/3 \
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
