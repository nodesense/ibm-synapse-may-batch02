```sql


CREATE DATABASE moviedb;

USE moviedb; 

-- create external file format
-- create data source
-- create exteranal table


CREATE EXTERNAL FILE FORMAT [ExternalCSVWithHeader] 
                WITH (
                    FORMAT_TYPE = DELIMITEDTEXT,
                    FORMAT_OPTIONS (FIELD_TERMINATOR=',', FIRST_ROW=2)
                )
 
 
CREATE EXTERNAL DATA SOURCE [movieset_ds]
  WITH (LOCATION='abfss://movieset@ibmbatch02synapse.dfs.core.windows.net')

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

SELECT * FROM ratings;


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

SELECT * from tags;




CREATE EXTERNAL TABLE movies  (
     id INT,
    title VARCHAR(500),
    genres VARCHAR(250)
) WITH (
    -- location/path within data source
    LOCATION='movies/movies.csv',
    -- data source has container name, storage account
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT * from movies;




-- GROUP BY, HAVING, ORDER BY 
-- external table with in dedicated sql

SELECT movieId, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
FROM ratings 
GROUP BY (movieId)
HAVING COUNT(userId) >= 100
ORDER BY total_voting DESC;


-- join external table from data lake with native dedi sql pool table

SELECT movieId, title, COUNT(userId) AS total_voting, AVG(rating) AS avg_rating 
FROM ratings 
JOIN movies 
ON movies.id = ratings.movieId
GROUP BY movieId, title
HAVING COUNT(userId) >= 100
ORDER BY total_voting DESC;

-- CETAS - Create External Table As Select

-- we create a dedi pool table called popular_movies
-- from the query 

CREATE EXTERNAL TABLE popular_movies 
WITH(
    LOCATION='popular-movies/popular.csv', 
    DATA_SOURCE = [movieset_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
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
