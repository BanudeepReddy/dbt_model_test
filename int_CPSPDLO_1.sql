WITH CTE_Competitor AS
(
select distinct competitorid,MasterAddressID,Company from {{source('ps_dap_summary','Competitor')}}
where Company != 'Public Storage'
)

, CTE_CompetitorPricing AS
(
    select pricedate, competitorpricingid, price, WebOnlyPrice, competitorid,competitorunitattributeid
    from {{source('ps_dap_summary','CompetitorPricing')}}
    where pricedate >(SELECT MAX(pricedate)  FROM {{source('ps_pricing_ds','CompetitorPriceSiteProductDistanceLessOutliers')}} )
    AND Ifnull(price,0) > 0
    AND Extract(Hour from PriceDate) = 0
)

, CTE_CompetitorUnitAttribute AS
(
    select competitorunitattributeid, psproductid
    from {{source('ps_dap_summary','CompetitorUnitAttribute')}}
    where  psproductid IN(1, 2, 3, 4, 5, 6, 7, 8) AND CAST(sqft as INT64) <= 350
)

, Tmp_Comp_Price_Load AS
(
    SELECT cp.pricedate,
                   cp.competitorpricingid,
                   cat.CategoryID,
                   cat.CategoryName,
                   cp.price,
                   p.WebChampSiteID AS SiteID,
                   p.MarketName,
                   NULL AS Stdev_Price,
                   NULL AS Avg_Price,
                   NULL AS Max_Price,
                   NULL AS Min_Price,
                   NULL AS Counts,
                   NULL AS Counts_Less_Outliers,
                   NULL AS Num_of_Observations,
                   psdm.Distance,
                   cp.WebOnlyPrice
            FFROM CTE_CompetitorPricing cp
            INNER JOIN CTE_Competitor c ON cp.competitorid = c.competitorid
            INNER JOIN CTE_CompetitorUnitAttribute cua ON cp.competitorunitattributeid = cua.competitorunitattributeid
                 INNER JOIN {{ source("ps_dap_summary", "PSCompetitorDistanceMatrix") }} psdm ON c.MasterAddressID = psdm.MasterAddressID
                 INNER JOIN {{ source("ps_dap_summary", "dim_PSProperty") }} p ON p.WebChampSiteID = psdm.PSWebChampSiteID--PSSJSQL001.psdatawh
                 INNER JOIN {{ source("ps_dap_summary", "Product") }} prod ON prod.ProductID = cua.psProductID
                 INNER JOIN {{ source("ps_dap_summary", "Category") }} cat ON cat.ProductSubTypeId = prod.ProductSubTypeId
            WHERE cua.psproductid IN(168) --  = @Product
                 AND cp.price IS NOT NULL
                 AND cua.sqft <= 350
                 AND cua.SqFt >= 5
                 AND psdm.Distance <= 5
                 AND cp.pricedate > @LastPricedate -- = @Pricedate
                 AND p.IsPropertyActive = 1
                 AND p.PropertyTypeCode = 'MI'
                 AND p.WebChampSiteID > 0
                 AND c.Company != 'Public Storage'
                 -- or c.Company is null)
                 --and c.company is null
                 AND DATEPART(hh, cp.PriceDate) = 0 --remove ExtraSpaceTest
                 AND cua.SqFT >= cat.MinSqft
                 AND cua.SqFT < cat.MaxSqft
            ORDER BY cp.price DESC;
)
SELECT * FORM Tmp_Comp_Price_Load
