import time

def isprime(n):
    if n < 2:
        return False   
    if n == 2:          
        return True   
    if not n & 1:       
        return False   
    for x in range(3, int(n**0.5)+1, 2):       
        if n % x == 0:           
            return False   
    return True

start_time = time.time()

from pyspark import SparkContext 
# sc = SparkContext(master="spark://192.168.0.117:7077") 

sc = SparkContext(master="local[*]") 
nums = sc.parallelize(range(10000))
val = nums.repartition(8).coalesce(5)
num_primes = nums.map(lambda x: isprime(x)) 
primes = num_primes.filter(lambda x: x)

print(primes.count())

print("--- %s seconds ---" % (time.time() - start_time))
