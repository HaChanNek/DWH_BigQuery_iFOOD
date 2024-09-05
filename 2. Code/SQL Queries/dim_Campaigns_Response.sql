CREATE OR REPLACE TABLE `datawarehouse-422504.OLAP.Dim_Campaign_Response` AS
SELECT DISTINCT
    Response_ID,
    CASE
        WHEN CAST(AcceptedCmp1 AS STRING) = 'True' THEN 1
        WHEN CAST(AcceptedCmp1 AS STRING) = 'False' THEN 0
        ELSE CAST(AcceptedCmp1 AS INTEGER)
    END AS AcceptedCmp1,
    CASE
        WHEN CAST(AcceptedCmp2 AS STRING) = 'True' THEN 1
        WHEN CAST(AcceptedCmp2 AS STRING) = 'False' THEN 0
        ELSE CAST(AcceptedCmp2 AS INTEGER)
    END AS AcceptedCmp2,
    CASE
        WHEN CAST(AcceptedCmp3 AS STRING) = 'True' THEN 1
        WHEN CAST(AcceptedCmp3 AS STRING) = 'False' THEN 0
        ELSE CAST(AcceptedCmp3 AS INTEGER)
    END AS AcceptedCmp3,
    CASE
        WHEN CAST(AcceptedCmp4 AS STRING) = 'True' THEN 1
        WHEN CAST(AcceptedCmp4 AS STRING) = 'False' THEN 0
        ELSE CAST(AcceptedCmp4 AS INTEGER)
    END AS AcceptedCmp4,
    CASE
        WHEN CAST(AcceptedCmp5 AS STRING) = 'True' THEN 1
        WHEN CAST(AcceptedCmp5 AS STRING) = 'False' THEN 0
        ELSE CAST(AcceptedCmp5 AS INTEGER)
    END AS AcceptedCmp5
FROM `datawarehouse-422504.OLTP.Campaign_Response`;
