```

-- Managed Access - use Active Directory
-- ExternalCSVWithHeader is already present 

-- access external storage which is not linked to synapse

CREATE EXTERNAL DATA SOURCE [movielens_ds]
  WITH (LOCATION='abfss://movielens@ibmbatch02storage.dfs.core.windows.net', 
        TYPE=HADOOP)


-- fails with External file access failed due to internal error: 'Error occurred while accessing HDFS: Java exception raised on call to HdfsBridge_IsDirExist. Java exception message: HdfsBridge::isDirExist - Unexpected error encountered checking whether directory exists or not: AbfsRestOperationException: Operation failed: "This request is not authorized to perform this operation using this permission.", 403, HEAD, https://ibmbatch02storage.dfs.core.windows.net/movielens/links?upn=false&action=getStatus&timeout=90'

-- Solution 1: Use Access Control, ensure to assign a role like Blob Contributor to 
--   synapse workspace ibmbatch02 to storage acccount container movielens


-- In the storage account, go to access control, Assign role,
   -- Contributor, search for ibm/ something relevant to your synapse workspace
   -- select from results
   -- save button

```

```sql
CREATE EXTERNAL TABLE movielens_links (
    movieId INT,
    imdbId INT,
    tmdbId INT
) WITH (
    -- location/path within data source, point to directory, not a specific file
    LOCATION='links/',
    -- data source has container name, storage account
    DATA_SOURCE = [movielens_ds],
    FILE_FORMAT = [ExternalCSVWithHeader]
);

SELECT * from movielens_links;
```
