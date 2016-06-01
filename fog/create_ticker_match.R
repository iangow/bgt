# 
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())
library(reshape)

dbGetQuery(pg, "
    -- Code for the ticker match
    SET work_mem='10GB';

    DROP TABLE IF EXISTS bgt.ticker_match;
    
    CREATE TABLE bgt.ticker_match AS
    WITH 
    linktable AS (
        SELECT gvkey, lpermno::integer AS permno, linkdt, linkenddt
        FROM crsp.ccmxpf_linktable 
        WHERE usedflag=1 AND linkprim IN ('C', 'P')),

    calls AS (
        -- Some tickers have *s in them, so I clean them out.
        -- Note that the call data is only from ~2001 through 2010
        -- so I condition on this below when merging with Compustat.
        -- Note that I just yesterday (2013-05-21) got call data
        -- through to 2013.
        SELECT regexp_replace(ticker, '[*]', '', 'g') AS ticker,
        file_name, co_name, call_desc, call_date::date
        FROM streetevents.calls),

    rdqs AS (
        -- Merge tickers from comp.secm with announcement dates
        -- from comp.fundq
        SELECT DISTINCT a.gvkey, b.tic, a.rdq, a.conm, a.datadate
        FROM comp.secm AS b
        RIGHT JOIN comp.fundq AS a
        ON a.gvkey=b.gvkey AND eomonth(b.datadate)=eomonth(a.rdq)),

    ticker_match AS (
        -- Match earnings announcements with calls within three days
        -- and with the same ticker
        SELECT a.*, b.gvkey, b.rdq, b.datadate
        FROM calls AS a
        INNER JOIN rdqs AS b
        ON a.ticker=b.tic AND 
        -- 2012-09-30 is the latest data on comp.secm. Assume that tickers after 
        -- that date are good.
        a.call_date BETWEEN b.rdq AND b.rdq + interval '3 days')
    -- Finally add PERMNO
    SELECT a.*, b.permno
    FROM ticker_match AS a
    LEFT JOIN linktable AS b
    ON a.gvkey=b.gvkey AND 
    a.datadate >= b.linkdt AND
    (a.datadate <= b.linkenddt OR b.linkenddt IS NULL);
    
    CREATE INDEX ON bgt.ticker_match (file_name);")

rs <- dbDisconnect(pg)