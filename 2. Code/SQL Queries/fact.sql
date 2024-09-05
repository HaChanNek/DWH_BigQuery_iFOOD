CREATE OR REPLACE TABLE `datawarehouse-422504.OLAP.fact_MarketingCampaignResponse` AS
WITH Campaign_Acceptance AS (
    SELECT
        cr.User_ID,
        MAX(dcr.AcceptedCmp1) AS AcceptedCmp1,
        MAX(dcr.AcceptedCmp2) AS AcceptedCmp2,
        MAX(dcr.AcceptedCmp3) AS AcceptedCmp3,
        MAX(dcr.AcceptedCmp4) AS AcceptedCmp4,
        MAX(dcr.AcceptedCmp5) AS AcceptedCmp5
    FROM
        `datawarehouse-422504.OLTP.Campaign_Response` cr
    LEFT JOIN `datawarehouse-422504.OLAP.Dim_Campaign_Response` dcr ON cr.Response_ID = dcr.Response_ID
    GROUP BY
        cr.User_ID
),
Product_Spend AS (
    SELECT
        pc.User_ID,
        MAX(pc.MntWines) AS Total_Wines,
        MAX(pc.MntFruits) AS Total_Fruits,
        MAX(pc.MntMeats) AS Total_Meats,
        MAX(pc.MntFishs) AS Total_Fish,
        MAX(pc.MntSweets) AS Total_Sweets,
        MAX(pc.MntGolds) AS Total_Golds
    FROM
        `datawarehouse-422504.OLTP.Product_Consumption` pc
    GROUP BY
        pc.User_ID
),
Total_Purchase AS (
    SELECT
        pur.User_ID,
        MAX(pur.NumWebPurchases) AS Total_Web_Purchases,
        MAX(pur.NumCatalogPurchases) AS Total_Catalog_Purchases,
        MAX(pur.NumStorePurchases) AS Total_Store_Purchases
    FROM
        `datawarehouse-422504.OLTP.Purchases` pur
    GROUP BY
        pur.User_ID
)
SELECT DISTINCT
    cp.User_ID,
    MIN(pur.Purchase_ID) AS Purchase_ID,
    MIN(pc.Consumption_ID) AS Consumption_ID,
    MIN(cr.Response_ID) AS Response_ID,
    MIN(dd.Response_Date_ID) AS Response_Date_ID,
    cp.Recency,
    MIN(com.Complain_Times) AS Complain_Times,
    MIN(pur.NumWebVisitsMonth) AS NumWebVisitsMonth,
    COALESCE(SUM(tp.Total_Web_Purchases), 0) + COALESCE(SUM(tp.Total_Catalog_Purchases), 0) + COALESCE(SUM(tp.Total_Store_Purchases), 0) AS Total_Purchases,
    COALESCE(MAX(ps.Total_Wines), 0) + COALESCE(MAX(ps.Total_Fruits), 0) + COALESCE(MAX(ps.Total_Meats), 0) + COALESCE(MAX(ps.Total_Fish), 0) + COALESCE(MAX(ps.Total_Sweets), 0) + COALESCE(MAX(ps.Total_Golds), 0) AS Total_Spent,
    CASE
        WHEN MIN(cr.Response) = 'True' THEN 1
        WHEN MIN(cr.Response) = 'False' THEN 0
        ELSE NULL
    END AS Response,
    COALESCE(MAX(ca.AcceptedCmp1), 0) + COALESCE(MAX(ca.AcceptedCmp2), 0) + COALESCE(MAX(ca.AcceptedCmp3), 0) + COALESCE(MAX(ca.AcceptedCmp4), 0) + COALESCE(MAX(ca.AcceptedCmp5), 0) AS Overall_Accept_Campaign,
    MIN(de.NumDealsPurchases) AS NumDealsPurchases
FROM
    `datawarehouse-422504.OLTP.Customer` cp
    LEFT JOIN `datawarehouse-422504.OLTP.Purchases` pur ON cp.User_ID = pur.User_ID
    LEFT JOIN `datawarehouse-422504.OLTP.Product_Consumption` pc ON cp.User_ID = pc.User_ID
    LEFT JOIN Product_Spend ps ON cp.User_ID = ps.User_ID
    LEFT JOIN Total_Purchase tp ON cp.User_ID = tp.User_ID
    LEFT JOIN `datawarehouse-422504.OLTP.Campaign_Response` cr ON cp.User_ID = cr.User_ID
    LEFT JOIN `datawarehouse-422504.OLAP.Dim_DateTime` dd ON SAFE_CAST(cr.Date_Response AS DATE) = dd.Date
    LEFT JOIN `datawarehouse-422504.OLTP.Complain` com ON cp.User_ID = com.User_ID
    LEFT JOIN `datawarehouse-422504.OLTP.Deals` de ON cp.User_ID = de.User_ID
    LEFT JOIN Campaign_Acceptance ca ON cp.User_ID = ca.User_ID
GROUP BY
    cp.User_ID, cp.Recency;
