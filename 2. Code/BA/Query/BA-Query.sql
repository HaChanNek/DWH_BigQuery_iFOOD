--Which country has the highest Marketing Campaign Acceptance Rate?
SELECT 
    dc.Country,
    SUM(fmc.Overall_Accept_Campaign) AS Total_Accepted,
    COUNT(fmc.Response) AS Total_Campaigns,
    (SUM(fmc.Overall_Accept_Campaign) / COUNT(fmc.Response)) * 100 AS Acceptance_Rate
FROM 
    `OLAP.fact_MarketingCampaignResponse` fmc
JOIN 
    `OLAP.Dim_Customer` dc ON fmc.User_ID = dc.User_ID
GROUP BY 
    dc.Country
ORDER BY 
    Acceptance_Rate DESC
LIMIT 8;


--Which customer segments have historically spent the most across different categories (e.g., MntWines, MntFruits)
SELECT 
    dc.User_ID,
    dc.Country,
    AVG(pc.MntWines) AS Avg_Wines,
    AVG(pc.MntFruits) AS Avg_Fruits,
    AVG(pc.MntMeats) AS Avg_Meats,
    AVG(pc.MntFishs) AS Avg_Fish,
    AVG(pc.MntSweets) AS Avg_Sweets,
FROM 
    `OLAP.fact_MarketingCampaignResponse` fmc
JOIN 
    `OLAP.Dim_Customer` dc ON fmc.User_ID = dc.User_ID
JOIN 
    `OLAP.Dim_Product_Consumption` pc ON fmc.Consumption_ID = pc.Consumption_ID
GROUP BY 
    dc.User_ID, dc.Country
ORDER BY 
    (Avg_Wines + Avg_Fruits + Avg_Meats + Avg_Fish + Avg_Sweets) DESC
LIMIT 10;

--What are the acceptance rates of marketing campaigns across different purchase methods (web, catalog, store) in each country?
SELECT 
    dc.Country,
    SUM(dp.NumWebPurchases) AS Total_Web_Purchases,
    SUM(dp.NumCatalogPurchases) AS Total_Catalog_Purchases,
    SUM(dp.NumStorePurchases) AS Total_Store_Purchases,
    SUM(fmc.Overall_Accept_Campaign) AS Total_Accepted,
    ROUND((SUM(fmc.Overall_Accept_Campaign) / SUM(dp.NumWebPurchases)) * 100, 2) AS Web_Acceptance_Rate,
    ROUND((SUM(fmc.Overall_Accept_Campaign) / SUM(dp.NumCatalogPurchases)) * 100, 2) AS Catalog_Acceptance_Rate,
    ROUND((SUM(fmc.Overall_Accept_Campaign) / SUM(dp.NumStorePurchases)) * 100, 2) AS Store_Acceptance_Rate
FROM 
    `OLAP.fact_MarketingCampaignResponse` fmc
JOIN 
    `OLAP.Dim_Customer` dc ON fmc.User_ID = dc.User_ID
JOIN 
    `OLAP.Dim_Purchases` dp ON fmc.Purchase_ID = dp.Purchase_ID
GROUP BY 
    dc.Country
ORDER BY 
    Total_Accepted DESC
LIMIT 10;

--How do factors like marital status, number of children at home (Kidhome, Teenhome), and education level affect customer purchasing habits and responsiveness to campaigns?
SELECT 
    dc.Marital_Status,
    dc.Education,
    dc.Kidhome,
    dc.Teenhome,
    SUM(fmc.Total_Spent) AS Total_Spent,
    SUM(fmc.Overall_Accept_Campaign) as Total_accepted,
    COUNT(fmc.Response) AS Total_Responses
FROM 
    `OLAP.fact_MarketingCampaignResponse` fmc
JOIN 
    `OLAP.Dim_Customer` dc ON fmc.User_ID = dc.User_ID
GROUP BY 
    dc.Marital_Status, dc.Education, dc.Kidhome, dc.Teenhome
ORDER BY 
    Total_accepted DESC
LIMIT 10;

--How does the frequency of web visits correlate with purchasing behavior and campaign responsiveness?
SELECT 
    dc.User_ID,
    fmc.NumWebVisitsMonth,
    SUM(fmc.Total_Spent) AS Total_Spent,
    SUM(fmc.Overall_Accept_Campaign) AS Total_Accepted
FROM 
    `OLAP.fact_MarketingCampaignResponse` fmc
JOIN 
    `OLAP.Dim_Customer` dc ON fmc.User_ID = dc.User_ID
GROUP BY 
    dc.User_ID, fmc.NumWebVisitsMonth
ORDER BY 
    fmc.NumWebVisitsMonth DESC
LIMIT 10;




