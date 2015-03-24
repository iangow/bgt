library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

fog_data <- dbGetQuery(pg,"
    SET work_mem='1GB';

    WITH link_table AS (
        SELECT DISTINCT ugvkey AS gvkey,
            apermno::integer AS permno
        FROM crsp.ccmxpf_lnkused),

    rdqs AS (
        SELECT DISTINCT gvkey, rdq
        FROM comp.fundq),

    rdq_link AS (
        SELECT *
        FROM link_table
        INNER JOIN rdqs
        USING (gvkey))

    SELECT c.permno, b.call_date::date, 
        a.*, d.r_squared, d.num_obs, d.constant, d.slope, 
        d.mean_analyst_fog, d.mean_manager_fog,
        e.gvkey, e.rdq
    FROM bgt.fog_recast AS a
    INNER JOIN streetevents.calls AS b
    USING (file_name)
    LEFT JOIN streetevents.crsp_link AS c
    USING (file_name)
    LEFT JOIN bgt.within_call_data AS d
    USING (file_name)
    LEFT JOIN rdq_link AS e
    ON c.permno=e.permno 
        AND b.call_date BETWEEN e.rdq AND e.rdq + interval '3 days'
    WHERE c.permno IS NOT NULL
")

# library(foreign)
# rs <- write.dta(dataframe =fog_data, file ="data/fog_data.dta")
save(fog_data, file="data/fog_data.Rdata")
system("/Applications/StatTransfer12/st data/fog_data.Rdata data/fog_data.sas7bdat -y")

fog_data_ticker <- dbGetQuery(pg,"
    SET work_mem='1GB';

    WITH 
    
    crsp_linktable AS (
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
        a.call_date BETWEEN b.rdq AND b.rdq + interval '3 days'),
    
    -- Finally add PERMNO
    link_table AS (
        SELECT a.*, b.permno
        FROM ticker_match AS a
        LEFT JOIN crsp_linktable AS b
        ON a.gvkey=b.gvkey AND 
            a.datadate >= b.linkdt AND
            (a.datadate <= b.linkenddt OR b.linkenddt IS NULL))

    SELECT e.permno, e.gvkey, e.rdq, b.call_date::date, 
        a.*, d.r_squared, d.num_obs, d.constant, d.slope, 
        d.mean_analyst_fog, d.mean_manager_fog
    FROM bgt.fog_recast AS a
    INNER JOIN streetevents.calls AS b
    USING (file_name)
    LEFT JOIN bgt.within_call_data AS d
    USING (file_name)
    LEFT JOIN link_table AS e
    USING (file_name)
    WHERE e.permno IS NOT NULL
")

# library(foreign)
save(fog_data_ticker, file="data/fog_data_ticker.Rdata")
system("/Applications/StatTransfer12/st data/fog_data_ticker.Rdata data/fog_data_ticker.sas7bdat -y")

dbDisconnect(pg)
rm(fog_data, fog_data_ticker, pg)