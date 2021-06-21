```sql

-- ExternalTablesCopyCSvJoinCTAS
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


 -- ratings: userId,movieId,rating,timestamp


CREATE EXTERNAL TABLE ratings (
    userId INT,
    movieId INT,
    rating FLOAT,
    timestamp BIGINT 
) WITH (
    -- location/path within data source, point to directory, not a specific file
    LOCATION='ratings/',
    -- data source has container name, storage account
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT TOP 10 * FROM ratings;


-- userId,movieId,tag,timestamp

CREATE EXTERNAL TABLE tags (
    userId INT,
    movieId INT,
    tag VARCHAR(256),
    timestamp BIGINT
) WITH (
    -- location/path within data source
    LOCATION='tags/tags.csv',
    -- data source has container name, storage account
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT TOP 10 * from tags;


-- movies table shall be created inside Dedipool as managed table
-- we shall copy the movies.csv into movies table of dedipool
-- movies is internal table, not external

-- create internal/managed table in dedicated pool

CREATE TABLE movies (
    id INT NOT NULL,
    title VARCHAR(500),
    genres VARCHAR(250)
) WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

-- copy all data from csv to movies internal table

COPY INTO movies 
    FROM 'https://ibmbatch02synapse.blob.core.windows.net/movieset/movies/movies.csv'
WITH (
FILE_TYPE = 'CSV',
FIELDTERMINATOR = ',',
FIRSTROW=2
);

SELECT * FROM  movies;

-- Synapse support PolyBase, query native tables, external tables,
--  sql servers, oracle, cosmos db, openrowset which support hadoo files etc
-- we can use them in joins etc

-- Aggregation with movieset

-- GROUP BY, HAVING, ORDER BY 
-- external table with in dedicated sql

SELECT movieId, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
FROM ratings 
GROUP BY (movieId)
HAVING COUNT(userId) >= 100
ORDER BY total_voting DESC;


-- Join with movies data, join external and internal data
-- join data lake tables with data warehouse tables



SELECT movieId, title, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
FROM ratings 
JOIN movies 
ON movies.id = ratings.movieId
GROUP BY movieId, title
HAVING COUNT(userId) >= 100
ORDER BY total_voting DESC;



-- CTAS - Create Table As Select

-- we create a dedi pool table called popular_movies, native/internal table
-- from the query 
-- statistical data 
-- analytical output

CREATE TABLE popular_movies 
WITH(
    DISTRIBUTION=REPLICATE, 
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT movieId, title, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
    FROM ratings 
    JOIN movies 
    ON movies.id = ratings.movieId
    GROUP BY movieId, title
    HAVING COUNT(userId) >= 100;

SELECT * from popular_movies;

```
