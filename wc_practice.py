import sys
import re
from operator import add

from pyspark import SparkContext

def map_phase(x):
    x = re.sub('--', ' ', x)
    x = re.sub("'", '', x)
    return re.sub('[?!@#$\'",.;:()]', '', x).lower()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.stderr.write("Usage: wordcount <master> <inputfile> <outputfile>")
        sys.exit(-1)
    sc = SparkContext(sys.argv[1], "python_wordcount_sorted in bigdataprogrammiing")
    lines = sc.textFile(sys.argv[2], 2)
    print(lines.getNumPartitions()) # print the number of partitions

    sys.stdout.write(f"{lines.filter(lambda x: x == 'Tokyo').count()}")
    outRDD = lines.map(lambda x: (map_phase(x), 1))\
        .reduceByKey(add) \
        .filter(lambda x: x[0] != 'tokyo')\
        .sortBy(lambda x: x[1])
    outRDD.saveAsTextFile(sys.argv[3])

