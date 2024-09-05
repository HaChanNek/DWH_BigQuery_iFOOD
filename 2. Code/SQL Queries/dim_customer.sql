
CREATE OR REPLACE TABLE `datawarehouse-422504.OLAP.Dim_Customer` AS
SELECT DISTINCT
    User_ID,
    Year_Birth,
    Education,
    Marital_Status,
    Income,
    Kidhome,
    Teenhome,
    Country
FROM `datawarehouse-422504.OLTP.Customer`;
