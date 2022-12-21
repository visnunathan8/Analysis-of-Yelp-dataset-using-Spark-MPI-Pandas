import pyspark
from pyspark import SparkContext

sc = SparkContext(master="local[*]")

data = sc.textFile("students.txt")

ans = data.getNumPartitions

print('Number of partitions before repartitioning: ',ans) 

data = data.repartition(4)

ans2 = data.getNumPartitions

print('Number of partitions after repartitioning: ',ans2) 

partitions = data.glom()

# Iterate over the partitions and print the data in each partition
for i, partition in enumerate(partitions.collect()):
    print('----------------------------')
    print(f"Partition {i}: {partition}")
