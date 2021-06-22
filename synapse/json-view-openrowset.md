```sql

-- working with JSON files

use moviedb;

-- read each line, read the content of each line , project as doc column of type nvarchar

select top 10 *
from openrowset(
        bulk 'https://ibmbatch02synapse.blob.core.windows.net/movieset/users/*.json',
        format = 'csv',
        fieldterminator ='0x0b',
        fieldquote = '0x0b'
    ) with (doc nvarchar(max)) as rows


-- after reading line content as json doc, now use dynamic expression to extract content

select
    JSON_VALUE(doc, '$.email') AS email,
    CAST(JSON_VALUE(doc, '$.id') AS INT) as id,
    doc
from openrowset(
        bulk 'https://ibmbatch02synapse.blob.core.windows.net/movieset/users/*.json',
        format = 'csv',
        fieldterminator ='0x0b',
        fieldquote = '0x0b'
    ) with (doc nvarchar(max)) as rows
 order by JSON_VALUE(doc, '$.id') desc


 ---
-- all columns

 
select
    JSON_VALUE(doc, '$.email') AS email,
    CAST(JSON_VALUE(doc, '$.id') AS INT) as id,
    JSON_VALUE(doc, '$.first') AS first,
    JSON_VALUE(doc, '$.last') AS last,
    JSON_VALUE(doc, '$.company') AS company, 
    JSON_VALUE(doc, '$.created_at') AS created_at, 
    JSON_VALUE(doc, '$.country') AS country
from openrowset(
        bulk 'https://ibmbatch02synapse.blob.core.windows.net/movieset/users/*.json',
        format = 'csv',
        fieldterminator ='0x0b',
        fieldquote = '0x0b'
    ) with (doc nvarchar(max)) as rows
 order by JSON_VALUE(doc, '$.id') desc

 --

 -- Working with Views, since external table doesn't support json, we can create views
 -- out of openrowset, then use them with joins etc...

-- DROP VIEW IF EXISTS peopleview;

CREATE VIEW peopleview AS
 SELECT
    CAST(JSON_VALUE(doc, '$.id') AS INT) as id,
    JSON_VALUE(doc, '$.email') AS email,
    JSON_VALUE(doc, '$.first') AS first,
    JSON_VALUE(doc, '$.last') AS last,
    JSON_VALUE(doc, '$.company') AS company, 
    JSON_VALUE(doc, '$.created_at') AS created_at, 
    JSON_VALUE(doc, '$.country') AS country
from openrowset(
        bulk 'https://ibmbatch02synapse.blob.core.windows.net/movieset/users/*.json',
        format = 'csv',
        fieldterminator ='0x0b',
        fieldquote = '0x0b'
    ) with (doc nvarchar(max)) as rows;


select * from peopleview;


select * from peopleview order by id desc;


```
