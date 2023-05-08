

{{
    config(
        materialized="incremental"
        ,partition_by={
            "field": "Pricedate",
            "data_type": "DATETIME",
            "granularity": "day"
        })
}}

{{ dbt_utils.log_info("Starting insertion for CompetitorPriceSiteProductDistanceLessOutliers") }}

WITH CTE_1 AS
(
SELECT Pricedate,
                   competitorpricingid,
                   psproductid,
                   price,
                   SiteID,
                   Counts_Less_Outliers,
                   Distance,
                   WebOnlyPrice,
                   NULL AS AdminFee,
                   NULL AS CompetitorID
            FROM {{ref('int_CPSPDLO_4')}}
            WHERE Counts_Less_Outliers = 1
)

, CTE_CompetitorPricing AS
(
    select competitorid, AdminFee,competitorpricingid
    from {{source('ps_dap_summary','CompetitorPricing')}}
    where pricedate >(SELECT MAX(pricedate)  FROM {{source('ps_pricing_ds','CompetitorPriceSiteProductDistanceLessOutliers')}} )

)
, CTE_Final  AS
(
Select c.* except(AdminFee, CompetitorID) ,
Case when cp.competitorpricingid is not null And c.AdminFee IS NULL AND c.PSProductID NOT IN(1, 2, 3, 4, 5, 6, 7, 8)
AND (c.Pricedate > (SELECT MAX(pricedate)  FROM {{source('ps_pricing_ds','CompetitorPriceSiteProductDistanceLessOutliers')}} ) )
 Then  cp.AdminFee
Else c.AdminFee End As AdminFee,
Case when cp.competitorpricingid is not null And c.AdminFee IS NULL AND c.PSProductID NOT IN(1, 2, 3, 4, 5, 6, 7, 8)
AND (c.Pricedate > (SELECT MAX(pricedate)  FROM {{source('ps_pricing_ds','CompetitorPriceSiteProductDistanceLessOutliers')}} ) )
 Then  cp.CompetitorID
Else c.CompetitorID End As CompetitorID
from CTE_1 c
LEFT JOIN CTE_CompetitorPricing cp
ON c.competitorpricingid = cp.competitorpricingid
)

select
Distinct
Datetime(Pricedate) as PriceDate,
CAST(CompetitorpricingID as INT64) as CompetitorpricingID,
CAST(PSProductID as INT64) as PSProductID,
CAST(Price as Numeric) as Price,
CAST(SiteID as INT64) as SiteID,
CAST(Counts_Less_Outliers as INT64) as Counts_Less_Outliers,
CAST(Distance as Numeric) as Distance,
CAST(WebOnlyPrice as Numeric) as WebOnlyPrice,
CAST(AdminFee as Numeric) as AdminFee,
CAST(CompetitorID as INT64) as CompetitorID
from CTE_Final
