CREATE OR REPLACE TABLE `datawarehouse-422504.OLAP.Dim_Product_Consumption` AS
SELECT distinct
    Consumption_ID,
    MntWines,
    MntFruits,
    MntMeats,
    MntFishs,
    MntSweets,
    MntGolds
FROM
    `datawarehouse-422504.OLTP.Product_Consumption`;
