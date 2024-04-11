from pyspark import SparkContext, SparkConf
import time
import random
import sys
 
conf = SparkConf()
workspace_id = None
if len(sys.argv) > 1:
    workspace_id = sys.argv[1]
    conf.setMaster(f'spark://spark-{workspace_id}-master-svc:7077')
 
sc = SparkContext(conf=conf)
 
NUM_SAMPLES = 3000000
 
def inside(_):
    x, y = random.random(), random.random()
    return x*x + y*y < 1
 
count = sc.parallelize(range(0, NUM_SAMPLES)).filter(inside).count()
 
conf_vals = sc.getConf().getAll()
executor_count = None
 
for val in conf_vals:
    print(f"{val[0]} {val[1]}")
    if val[0] == 'spark.executor.instances':
        executor_count = val[1]
 
pi_str = f"Pi is roughly {(4.0 * count / NUM_SAMPLES)}"
print(pi_str)
 
executor_str = f"Running spark job using {executor_count} executors"
print(executor_str)