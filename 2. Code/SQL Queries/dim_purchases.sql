CREATE OR REPLACE TABLE `datawarehouse-422504.OLAP.Dim_Purchases` AS
SELECT DISTINCT
    Purchase_ID,
    NumWebPurchases,
    NumCatalogPurchases,
    NumStorePurchases
FROM
    `datawarehouse-422504.OLTP.Purchases`;
