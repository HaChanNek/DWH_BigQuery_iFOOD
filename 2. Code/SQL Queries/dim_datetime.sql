CREATE OR REPLACE TABLE `datawarehouse-422504.OLAP.Dim_DateTime` AS
WITH Date_CTE AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY CAST(Date_Response AS DATE)) AS Response_Date_ID,
        CAST(Date_Response AS DATE) AS Date,
        EXTRACT(DAY FROM CAST(Date_Response AS DATE)) AS Day,
        EXTRACT(MONTH FROM CAST(Date_Response AS DATE)) AS Month,
        EXTRACT(QUARTER FROM CAST(Date_Response AS DATE)) AS Quarter,
        EXTRACT(YEAR FROM CAST(Date_Response AS DATE)) AS Year
    FROM `datawarehouse-422504.OLTP.Campaign_Response`
)
SELECT
    Response_Date_ID,
    Date,
    Day,
    Month,
    Quarter,
    Year
FROM Date_CTE;
