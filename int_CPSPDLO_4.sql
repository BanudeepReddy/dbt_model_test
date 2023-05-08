WITH CTE_1 AS
(
    select * from {{ref('int_CPSPDLO_3')}}
    where Round(Distance,1)<=2.5 AND  Num_of_Observations <= 10
)

, CTE_2 AS
(
    SELECT SiteID,
        psproductid,
        Pricedate,
        COUNT(DISTINCT Price) AS records
    FROM CTE_1
    GROUP BY SiteID,
            PSProductID,
            Pricedate
)

, CTE_3 AS
(
    select c.* except(Num_of_Observations),
    Case when t.SiteID is not Null AND t.psproductid is not null AND t.Pricedate is not null
         then t.Records
         Else c.Num_of_Observations
    End AS Num_of_Observations
    from CTE_1 c
    Left Join CTE_2 t
    on c.SiteID = t.SiteID
    AND c.psproductid = t.psproductid
    AND c.Pricedate = t.Pricedate
)

select * from CTE_3
