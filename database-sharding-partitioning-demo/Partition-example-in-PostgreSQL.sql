------ CREATION SCRIPT ---------
CREATE TABLE Sales (
    SaleID SERIAL,
    ProductID INT,
    SaleDate DATE,
    Amount DECIMAL(10, 2),
    PRIMARY KEY (SaleDate, SaleID)
) PARTITION BY RANGE (SaleDate);

-- Historical data partition
CREATE TABLE Sales_historical PARTITION OF Sales
    FOR VALUES FROM (MINVALUE) TO ('2023-01-01');

-- Recent data partition
CREATE TABLE Sales_recent PARTITION OF Sales
    FOR VALUES FROM ('2023-01-01') TO (MAXVALUE);

   
   
-- Insert data into the partitioned table
INSERT INTO Sales (ProductID, SaleDate, Amount) VALUES (1, '2022-05-15', 150.00);
INSERT INTO Sales (ProductID, SaleDate, Amount) VALUES (2, '2023-03-20', 200.00);   

-- Retrieval
select * from sales; -- 2 records
select * from sales_recent; -- 1 record
select * from sales_historical; -- 1 record 

SELECT * FROM Sales WHERE SaleDate BETWEEN '2022-01-01' AND '2023-12-31';

-- Calculate total sales for each partition
SELECT tableoid::regclass AS partition_name, SUM(Amount) AS total_sales FROM sales GROUP BY tableoid;

select tableoid::regClass,* from Sales;

