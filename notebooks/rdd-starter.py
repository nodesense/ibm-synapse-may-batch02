# Databricks notebook source
# this program is a spark application
# every spark is called spark driver
# driver will have 1 SparkContext
# sparkcontext shall manage rdd, cluster etc
sc

# COMMAND ----------

# create rdd from memory data
data = [1,2,3,4,5,6,7,8,9]
# load data into spark cluster
# lazy , data is not loaded to memory yet
rdd = sc.parallelize(data)

# COMMAND ----------

# we want filter out all odd numbers

# lineage, inherit rdd from parent rdd
# transform, lazy method, means, this code is not executed at all
rdd2 = rdd.filter (lambda n: n % 2 == 0) # remove all odd numbers

# COMMAND ----------

# multiply the n with 10
# map is a transform
# map doesn't executed immediately

rdd3 = rdd2.map (lambda n: n * 10)

# COMMAND ----------

# collect the output using action method

output = rdd3.collect() #action method
print(output)

# COMMAND ----------

names = ["python", "java", "pyspark", "jvm", "an", "a"]

# filter out the words len less than 4 chars
# convert the rest of the values to CAPITALIZE format. string.   upper()
# then collect and print the result


output = sc.parallelize(names)\
           .filter (lambda s: len(s) >= 4)\
           .map(lambda s: s.upper())\
           .collect()

print(output)

# COMMAND ----------


