```sql

use moviedb;


SELECT * FROM ratings;

select * from sys.external_file_formats;

-- already present in the external file format

-- CREATE EXTERNAL FILE FORMAT PARQUET_FORMAT
-- WITH
-- (  
--     FORMAT_TYPE = PARQUET,
--     DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
-- )


-- already present in the db

SELECT * FROM sys.external_data_sources;

-- CREATE EXTERNAL DATA SOURCE [movieset_ds]
--   WITH (LOCATION='abfss://movieset@ibmbatch02synapse.dfs.core.windows.net')


-- create external table as pqrquet
-- convert csv to parquet format

CREATE EXTERNAL TABLE ratings_parquet  WITH (
    LOCATION = '/ratings-parquet',
    DATA_SOURCE = movieset_ds,  
    FILE_FORMAT = PARQUET_FORMAT
) AS 
SELECT * FROM ratings;


SELECT * FROM ratings_parquet;




```
