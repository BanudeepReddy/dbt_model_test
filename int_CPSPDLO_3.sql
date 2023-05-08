with CTE_1 AS
(
SELECT * FROM {{ref('int_CPSPDLO_2')}}
WHERE Counts_Less_Outliers <> 0
)

,updateNum_of_Observations  AS
(SELECT COUNT(DISTINCT Price) AS Num_of_Observations,
                     SiteID,
                     PSProductID,
                     Pricedate
              FROM CTE_1
              GROUP BY SiteID,
                       PSProductID,
                       PriceDate
)

, CTE_2 AS
(SELECT t.* except(Num_of_Observations),ifnull(u.Num_of_Observations, Num_of_Observations) as Num_of_Observations
         FROM CTE_1 t
         LEFT JOIN updateNum_of_Observations u
         ON t.PriceDate = u.PriceDate AND t.psproductid = u.psproductID AND t.pricedate = u.pricedate;
)
----------------------CURSOR
, tmpv AS
(SELECT Pricedate,
            MarketName,
            SiteID,
            psproductid,
            STDEV(Price) AS stdev_Price,
            AVG(Price) AS avg_Price,
            MAX(Price) AS Max_Price,
            MIN(Price) AS Min_Price,
            COUNT(*) AS records

     FROM CTE_1 c     ------------------------------------------------
     GROUP BY Pricedate,
              MarketName,
              SiteID,
              PSProductID
)

, tmpv1 AS
(SELECT Pricedate,
            MarketName,
            PSProductID,
            STDEV(Price) AS stdev_Price,
            AVG(Price) AS avg_Price,
            MAX(Price) AS Max_Price,
            MIN(Price) AS Min_Price,
            COUNT(*) AS records
     FROM CTE_1                    -------------------------------
     GROUP BY Pricedate,
              MarketName,
              PSProductID
)

, tmp_StatTablev AS
(SELECT t.Pricedate,
            t.SiteID,
            t.PSProductID,
            t.stdev_Price,
            t.avg_Price,
            t.Max_Price,
            t.Min_Price,
            t.Records AS Recordsadj,
            CASE
                WHEN t.Records < 2
                THEN t1.stdev_Price * .7
                ELSE t.stdev_Price * .7
            END AS stdev_priceadj,
            CASE
                WHEN t.Records < 2
                THEN t1.avg_Price
                ELSE t.avg_Price
            END AS avg_priceadj,
            CASE
                WHEN t.Records < 2
                THEN t1.avg_Price + (t1.stdev_Price * .7)
                ELSE t.avg_Price + (t.stdev_Price * .7)
            END AS Max_priceadj,
            CASE
                WHEN t.Records < 2
                THEN t1.avg_Price - (t1.stdev_Price * .7)
                ELSE t.Avg_Price - (t.stdev_Price * .7)
            END AS Min_priceadj
     FROM tmpv t
          INNER JOIN tmpv1 t1 ON t1.Marketname = t.MarketName
                                  AND t1.PSProductid = t.PSProductID
                                  AND t1.Pricedate = t.Pricedate
)

, Tmp_Comp_Price_Final AS
(SELECT c.Pricedate,
                   c.competitorpricingid,
                   c.psproductid,
                   c.Name,
                   c.price,
                   c.SiteID,
                   c.MarketName,
                   CASE
                       WHEN t.stdev_priceadj > 200
                       THEN t.stdev_Price
                       ELSE t.stdev_priceadj
                   END AS Stdev_Price,
                   t.avg_priceadj AS Avg_Price,
                   t.avg_priceadj + t.stdev_priceadj AS Max_Price,
                   CASE
                       WHEN t.avg_priceadj - t.stdev_priceadj < 11
                       THEN 11
                       ELSE t.avg_priceadj - t.stdev_priceadj
                   END AS Min_Price,
                   1 AS Counts,
                   CASE
                       WHEN(comp.Name LIKE 'life Storage%'
                            OR comp.Name LIKE 'CubeSmart%'
                            OR comp.Name LIKE 'Extra Space%')
                       THEN 1
                       WHEN c.price < CASE
                                          WHEN t.avg_priceadj - t.stdev_priceadj < 11
                                          THEN 11
                                          ELSE t.avg_priceadj - t.stdev_priceadj
                                      END
                       THEN 0
                       WHEN c.Price > t.avg_priceadj + t.stdev_priceadj
                       THEN 0
                       ELSE 1
                   END AS Counts_Less_Outliers,
      Recordsadj AS Num_of_Observations,
                   c.Distance,
                   c.WebOnlyPrice
            FROM CTE_1 c
                 JOIN {{ source("ps_dap_summary", "competitorpricing") }} cp ON cp.CompetitorPricingID = c.competitorpricingid  -----------
                 JOIN {{ source("ps_dap_summary", "competitor") }} comp ON comp.CompetitorID = cp.CompetitorID                  -----------
                 INNER JOIN tmp_StatTablev t ON c.SiteID = t.SiteID
                                                 AND c.PSProductID = t.PSProductID
                                                 AND c.Pricedate = t.Pricedate
            ORDER BY c.Pricedate ASC,
                     c.SiteID ASC,
                     c.PsProductID ASC
)

, Tmp_Comp_Price_Final_2 AS
(
         SELECT * except(Counts_Less_Outliers),ifnull(t.Counts_Less_Outliers, Counts_Less_Outliers) as Counts_Less_Outliers
         FROM Tmp_Comp_Price_Final f
         LEFT JOIN {{ref('int_CPSPDLO_2')}} t
         ON f.siteID = t.Siteid
                                                   AND f.psProductID = t.psProductID
                                                   AND f.competitorpricingID = t.competitorPricingID
                                                   AND f.Pricedate = t.Pricedate
         WHERE f.Counts_Less_Outliers = 1
           AND t.Counts_Less_Outliers = 0
)

, Tmp_Comp_Price_Final_3 AS
( select * except(Counts_Less_Outliers),
    case when Counts_Less_Outliers = 1  AND price > 500  AND Num_of_Observations = 1 then 0
    Else Counts_Less_Outliers
    End As Counts_Less_Outliers
 from  Tmp_Comp_Price_Final_2
)

SELECT * FROM Tmp_Comp_Price_Final_3
