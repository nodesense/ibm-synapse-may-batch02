```sql
-- create external tables - links, ratings, tags

-- create internal, copy data from csv into internal table - movies


CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeader] 
                WITH (
                    FORMAT_TYPE = DELIMITEDTEXT,
                    FORMAT_OPTIONS (FIELD_TERMINATOR=',', FIRST_ROW=2)
                );

SELECT * from sys.external_data_sources;

-- DROP  EXTERNAL FILE FORMAT [ExternalCSVWithHeader] 
-- check in your data ui

-- Create Data Source  data located in externally
-- Data Source contains connections, any auth settings if any

-- abfss - Azure blobs File System Secured
-- DFS - Distributed File System, Gen 2, ADLS compatible end point

CREATE EXTERNAL DATA SOURCE [movieset_ds]
  WITH (LOCATION='abfss://movieset@ibmbatch02synapse.dfs.core.windows.net', 
        TYPE=HADOOP)

-- DROP EXTERNAL DATA SOURCE [movieset_ds]

SELECT * from sys.external_data_sources;

-- Now we create external table, means, table meta data shall be with dedicated pool/DW
-- and the actual data is stored in storage lake 
-- to add data, add new file into lake storage
-- to remove, remove the content from lake storage



-- links movieId,imdbId,tmdbId

CREATE EXTERNAL TABLE links (
    movieId INT,
    imdbId INT,
    tmdbId INT
) WITH (
    -- location/path within data source, point to directory, not a specific file
    LOCATION='links/',
    -- data source has container name, storage account
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT * from links;


```
