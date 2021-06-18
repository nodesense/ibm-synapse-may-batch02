# Databricks notebook source
# spark has database
# database = metadata [separately] + data [separately]
# apache hive - datawarehouse on hadoop
# defacto standard for the meta data..
# metadata = [database name, tables properties inside database, the data location in HDFS, DBFS, S3, ADLS...]
# spark uses hive meta data

# COMMAND ----------

# 1. Spark Temp tables/Temp Views [spark session]
# 2. Spark tables [permanent table, both meta data + data is managed by spark]
# 3. spark external tables [permananet table, meta is managed by spark, data is managed by outside /ETL]

# COMMAND ----------

# Databricks notebook source

from pyspark.sql.types import StructType, LongType, StringType, IntegerType, DoubleType

# create new schema, add columns and datatypes, the last column is nullable or not..
movieSchema = StructType()\
         .add("movieId", IntegerType(), True)\
         .add("title", StringType(), True)\
         .add("genres", StringType(), True)\

movieDf = spark.read.format("csv")\
               .option("header", True)\
               .schema(movieSchema)\
               .load('/mnt/movielens/movies/movies.csv')

movieDf.printSchema() 

# COMMAND ----------

# ratingSchema and load ratings.csv
ratingSchema = StructType()\
         .add("userId", IntegerType(), True)\
         .add("movieId", IntegerType(), True)\
         .add("rating", DoubleType(), True)\
         .add("timestamp", StringType(), True)


ratingDf = spark.read.format("csv")\
               .option("header", True)\
               .schema(ratingSchema)\
               .load('/mnt/movielens/ratings/ratings.csv')

ratingDf.printSchema()

# COMMAND ----------

# Temp table/view out of dataframe
movieDf.createOrReplaceTempView("movies")
ratingDf.createOrReplaceTempView("ratings")

# COMMAND ----------

df = spark.sql("Select movieId, title from movies")
df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- majic function, code below taken and executed inside spark.sql, result is printed in display
# MAGIC SELECT movieId, title FROM movies

# COMMAND ----------

df = spark.sql("""
SELECT movieId, 
       count(userId) as total_ratings,
       avg(rating) as avg_rating
FROM   ratings
GROUP BY movieId
HAVING  total_ratings >= 100
ORDER BY total_ratings DESC
""")

df.show(100)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW popular_movies AS 
# MAGIC SELECT movieId, 
# MAGIC        count(userId) as total_ratings,
# MAGIC        avg(rating) as avg_rating
# MAGIC FROM   ratings
# MAGIC GROUP BY movieId
# MAGIC HAVING  total_ratings >= 100
# MAGIC ORDER BY total_ratings DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM popular_movies

# COMMAND ----------

# get the view/table as dataframe
df = spark.table("popular_movies")
df.printSchema()


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- show tables in current use db
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW TABLES IN  default

# COMMAND ----------


