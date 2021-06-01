# Databricks notebook source
data = range(1,10)

rdd = sc.parallelize(data)
sc.setLogLevel("WARN") # log WARN or ERROR, DO NOT Log INFO or DEBUG

# COMMAND ----------

def filterFunc(n):
  print ("--filter", n)
  return n % 2 == 0 # return True for even numbers

rdd2 = rdd.filter(filterFunc)

# COMMAND ----------

def mapFunc(n):
  print("**map", n)
  return  n * 10

rdd3 = rdd2.map(mapFunc)

# COMMAND ----------

output = rdd3.collect()
print(output)

# COMMAND ----------

# running another action

print (rdd3.min()) # will load again from sc.parallelize, rdd2/filter, rdd3/map and print the result with min

# COMMAND ----------

# action method
print (rdd3.max()) # will load again from sc.parallelize, rdd2/filter, rdd3/map and print the result with max

# COMMAND ----------

#every action method shall trigger spark job, execute the jobs
# Every Action, RDD => DAG => DAG Scheduler => Convert DAG into Stages => stages split into tasks => Task Queue => Task Exeuctor => Run the tasks on workers


# some RDD can be reused with cache/persist..
