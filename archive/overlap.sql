WITH mult_tickers AS (
  SELECT ticker, count(*)
  FROM bgt.tickers
  WHERE ticker !=''
  GROUP BY ticker
  HAVING count(*) > 1),
overlap_data AS (
  SELECT *, 
    bdate < lag(edate) OVER (PARTITION BY ticker ORDER BY bdate) AS overlap_b,
    edate > lead(bdate) OVER (PARTITION BY ticker ORDER BY bdate) AS overlap_f
  FROM bgt.tickers
  INNER JOIN mult_tickers
  USING (ticker))
SELECT *
FROM overlap_data
WHERE overlap_b OR overlap_f
ORDER BY ticker, bdate;
