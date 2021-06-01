# Databricks notebook source
# /FileStore/tables/sales.csv
# check the file path in dbfs, by visiting data , -> create table, dbfs tab
fileRdd = sc.textFile("/FileStore/tables/sales_2.csv")

header = fileRdd.first()
print (header)
 # skip first line
# ascending order
finalRdd = fileRdd.filter (lambda line: line != header)\
       .map (lambda line: line.strip().split(","))\
       .map (lambda arr: ( int(arr[0]), float(arr[1]), arr[2] ))\
       .sortBy(lambda t: t[1]) # sort by amount
       
             
print(finalRdd.collect())
 

# COMMAND ----------

# Decending order, for sort Rdd
# /FileStore/tables/sales.csv
# check the file path in dbfs, by visiting data , -> create table, dbfs tab
fileRdd = sc.textFile("/FileStore/tables/sales_2.csv")

header = fileRdd.first()
print (header)
 # skip first line
# ascending order
finalRdd = fileRdd.filter (lambda line: line != header)\
       .map (lambda line: line.strip().split(","))\
       .map (lambda arr: ( int(arr[0]), float(arr[1]), arr[2] ))\
       .sortBy(lambda t: t[1], False) # sort by amount, Decending order
       
             
print(finalRdd.collect())

# COMMAND ----------


