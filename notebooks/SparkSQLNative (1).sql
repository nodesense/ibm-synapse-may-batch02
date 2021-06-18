-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

CREATE DATABASE orderdb

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

-- managed table= > table, and data ideally to be managed by spark
-- persisted/permanent tables
-- dbname.tablename
CREATE TABLE orderdb.orders (id INT, amount INT)

-- COMMAND ----------

INSERT INTO orderdb.orders VALUES (1,100), (2,200), (3,300)

-- COMMAND ----------

SELECT * FROM orderdb.orders

-- COMMAND ----------


