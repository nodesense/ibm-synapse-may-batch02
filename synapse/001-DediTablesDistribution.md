```

-- DediTablesDistribution.md
-- Distribution ROUND_ROBIN, HASH, REPLICATE

-- dimension table/master data
-- REPLICATE - Table is replicated in all compute nodes
CREATE TABLE category (
    id INT NOT NULL,
    category_name VARCHAR(100)
) WITH (
    DISTRIBUTION = REPLICATE
)

INSERT INTO category VALUES (1, 'mobile');
INSERT INTO category VALUES (2, 'tablet');

SELECT * FROM category;

-- fact table, transaction table
-- Distribution  Hash
-- Distribute the data based on hash code
-- hash(country) % number of distribution 
-- the data shal be assigned to compute node based on hash output
CREATE TABLE sales (id INT NOT NULL,
    amount FLOAT, 
    country VARCHAR(100),
    category INT
    ) WITH (
        DISTRIBUTION = Hash(country)
    )


INSERT INTO sales VALUES (1, 100, 'IN', 1);
INSERT INTO sales VALUES (2, 200, 'USA', 2);
INSERT INTO sales VALUES (3, 300, 'CA', 1);
INSERT INTO sales VALUES (4, 600, 'IN', 2);
INSERT INTO sales VALUES (5, 700, 'CA', 2);
INSERT INTO sales VALUES (6, 800, 'CA', 1);

EXPLAIN select * from sales;


-- Distribution Round Robin
-- The data is distributed evenly without any logic across cluster
-- intermitten table.. 
-- staging tables
-- mostly deal these data in data lake itself.
CREATE TABLE page_clicks (
    page_id INT NOT NULL,
    user_id INT NOT NULL,
    click_time DATETIME
) WITH (
    DISTRIBUTION= ROUND_ROBIN
)

INSERT INTO page_clicks VALUES (2, 100, '2021-01-01T01:01:01');

SELECT * FROM page_clicks;

```
