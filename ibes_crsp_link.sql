SET work_mem='2GB';

WITH 
ibes1 AS (
    SELECT ticker, cusip, cname, sdates
    FROM ibes.idsum
    WHERE usfirm=1 AND cusip IS NOT NULL),
ibes_names AS (
    SELECT ticker, cusip, last_value(cname) OVER w AS cname
    FROM ibes1
    WINDOW w AS (PARTITION BY ticker, cusip ORDER BY sdates)),
ibes2 AS (
    SELECT ticker, cusip, min(sdates) AS fdate, max(sdates) AS ldate
    FROM ibes1
    GROUP BY ticker, cusip),
ibes3 AS (
    SELECT *
    FROM ibes2
    INNER JOIN ibes_names
    USING (ticker, cusip)),
crsp1 AS (
    SELECT permno, ncusip, min(namedt) AS namedt, max(nameenddt) AS nameenddt
    FROM crsp.stocknames
    WHERE ncusip IS NOT NULL
    GROUP BY permno, ncusip),
crsp_names AS (
   SELECT DISTINCT permno, ncusip,
       last_value(namedt) OVER w AS namedt,
       last_value(comnam) OVER w AS comnam
   FROM crsp.stocknames
   WHERE ncusip IS NOT NULL
   WINDOW w AS (PARTITION BY permno, ncusip ORDER BY namedt)),
crsp2 AS (
   SELECT DISTINCT *
   FROM crsp1
   INNER JOIN crsp_names
   USING (permno, ncusip, namedt)),
link1_1 AS (
   SELECT *
   FROM ibes3 AS a
   INNER JOIN crsp2 AS b
   ON a.cusip=b.ncusip),
link1_2 AS (
    SELECT DISTINCT ticker, permno, cname, comnam, 
    -- levenshtein(cname, comnam) AS name_dist,
    CASE WHEN NOT((ldate <= namedt) OR (fdate>= nameenddt)) AND levenshtein(cname, comnam) <30 THEN 0
	WHEN NOT((ldate <= namedt) OR (fdate>= nameenddt)) THEN 1
	WHEN levenshtein(cname, comnam) <30 THEN 2
	ELSE 3 END AS score
    --a.permno, a.ticker, a.fdate, a.ldate,
    --b.comnam, b.namedt
    FROM link1_1 AS a),
link1_3 AS (
    SELECT ticker, permno, min(score) AS score
    FROM link1_2 
    GROUP BY ticker, permno
    ORDER BY ticker, permno),
nomatch1 AS (
    SELECT DISTINCT ticker
    FROM ibes1
    WHERE ticker NOT IN (SELECT ticker FROM link1_3)