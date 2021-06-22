Source File: https://github.com/nodesense/deloitte-azure-db-synapse-may-2021/blob/main/serverless-sql/part-00000-2cb993fc-1383-4c09-a452-4749657406ce-c000.snappy.parquet


```sql

-- serverless sql with external table parqeut format

use moviedb;

-- we already created datasource

create external data source popular_movies_ds
with ( location = 'abfss://movieset@ibmbatch02synapse.dfs.core.windows.net' );

CREATE EXTERNAL FILE FORMAT PARQUET_FORMAT
WITH
(  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)
 
/*
columns stored in parquet file already

movieId
total_users
avg_rating
title
*/
--   EXTERNAL TABLE  popular_movies_parquet ;
 
CREATE EXTERNAL TABLE popular_movies_parquet (
    movieId INT,
    total_users BIGINT,
    avg_rating FLOAT,
    title VARCHAR(100) COLLATE Latin1_General_100_BIN2_UTF8 
)  WITH (
    LOCATION = '/popular-movies-parquet',
    DATA_SOURCE = popular_movies_ds,  
    FILE_FORMAT = PARQUET_FORMAT
);

select * from popular_movies_parquet;

```
