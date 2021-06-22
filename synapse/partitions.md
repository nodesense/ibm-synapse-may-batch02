# Partititions 

```
-- how the data is split into subset in the distribution
-- helps the system to improves the queries
-- DB scan efficiancy is improved

-- Range partititions 

--    (10, 20, 30) - 4 partitions
      --  P0 (< 10 )
      --  P1   ( > 10 and <= 20)
      -- P2     (20 > and <= 30)
      -- P3 ( > 30)

    (0,10), (11, 20) (21, 30)


-- Range left
 --   the actual boundary value belongs to left partition, the last value in the left partition

   --   (1000, 2000, 3000, 4000, 5000) - range 

   --   Partition 0            <= 1000
   -- Partition  1 (1001 to 2000) >1000 and <= 2000
   -- Partition 2 (2001 to 3000) >2000 and <= 3000
   -- Partition  3 (3001 to 4000) >3000 and <= 4000
   -- Partition  4 (4001 to 5000) >4000 and <= 5000
   -- Partition  5 (4001 to 5000) >5000

-- Range right
    -- the actual boundary value belongs to right partition, first value in right partition

  --   Partition 0            < 1000
   -- Partition  1 (1000 to 1999) >=1000 and < 2000
   -- Partition 2 (2000 to 2999) >=2000 and < 3000
   -- Partition  3 (3000 to 3999) >=3000 and < 4000
   -- Partition  4 (4000 to 4999) >=4000 and < 5000
   -- Partition  5 ( >5000 )
   
```

```sql

CREATE TABLE customers (
    id INT NOT NULL,
    name varchar(100),
    gender varchar(10),
    email varchar(100),
    phone varchar(100)
) WITH (
    -- include distribution and cluster index types here
    PARTITION (id RANGE LEFT FOR VALUES (1000, 2000, 3000, 4000, 5000))
);


INSERT INTO customers VALUES(1, 'joe', 'male', 'joe@example.com', '1111111111');
INSERT INTO customers VALUES(2, 'mary', 'female', 'mary@example.com', '1111111112');

```
