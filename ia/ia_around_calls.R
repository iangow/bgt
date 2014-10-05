library(RPostgreSQL)
drv <- dbDriver("PostgreSQL")
pg <- dbConnect(drv, dbname="crsp")
            
system.time(ia_calls <- dbGetQuery(pg, "
  SET work_mem='20GB';
  
  WITH ticker_match AS (
    SELECT DISTINCT a.file_name, b.permno, b.comnam, a.call_date::date
    FROM streetevents.calls AS a
    LEFT JOIN crsp.stocknames AS b
    ON a.ticker=b.ticker AND 
      a.call_date::date BETWEEN b.namedt AND b.nameenddt
    WHERE call_type=1),
  amihud AS (
    SELECT permno, date,
        CASE WHEN abs(vol*prc) > 0 THEN abs(ret/(vol*100*prc)) END AS illiq
    FROM crsp.dsf),
  merged_fog_data AS (
    SELECT DISTINCT *,  
      ntile(5) over (
        PARTITION BY extract(year FROM call_date) 
        ORDER BY fog_comp_pres) AS quintile_fog_comp_pres,
      ntile(5) over (
        PARTITION BY extract(year FROM call_date) 
        ORDER BY fog_anal_qa) AS quintile_fog_anal_qa,
      ntile(5) over (
        PARTITION BY extract(year FROM call_date) 
        ORDER BY fog_comp_qa) AS quintile_fog_comp_qa
    FROM ticker_match
    INNER JOIN bgt.fog_recast
    USING (file_name)),
  ticker_date_w_td AS (
    SELECT DISTINCT a.*, b.td 
    FROM merged_fog_data AS a
    INNER JOIN crsp.anncdates AS b
    ON a.call_date=b.anncdate),
  taq_master AS (
      SELECT symbol, substr(cusip, 1, 8) AS cusip, fdate, 
          lead(fdate) OVER w AS lead_fdate
      FROM taq.mast WHERE fdate IS NOT NULL
      WINDOW w AS (PARTITION BY symbol ORDER BY fdate)),
  ia_daily AS (
        SELECT DISTINCT a.*, b.cusip, c.permno
        FROM bgt.lambda_all AS a
        INNER JOIN taq_master AS b
        ON a.symbol=b.symbol AND 
            ((a.date BETWEEN b.fdate AND b.lead_fdate) OR 
            (a.date >= b.fdate AND b.lead_fdate IS NULL))
        LEFT JOIN crsp.stocknames AS c 
        ON b.cusip=c.ncusip),
  lambda_all AS (
      SELECT a.*, b.illiq, 
        ntile(10) OVER (
          PARTITION BY extract(year FROM b.date) 
          ORDER BY b.illiq) AS illiq_decile
      FROM ia_daily AS a
      INNER JOIN amihud AS b
      USING (permno, date)),
  raw_data AS (
    SELECT a.*, extract(year FROM a.date) AS year, 
      -- a.date, a.lambda_gh AS lambda_gh, a.ntrades, 
      twspread/twprice AS spread, c.td - b.td AS td, b.*
    FROM lambda_all AS a
    INNER JOIN crsp.anncdates AS c
    ON a.date=c.anncdate
    INNER JOIN ticker_date_w_td AS b
    ON a.permno=b.permno AND c.td - b.td BETWEEN - 15 AND 15)
  SELECT *
  FROM raw_data
  -- WHERE symbol='ORCL'"))
ia_calls$year <- format(ia_calls$call_date, "%Y")
# ia_calls <- subset(ia_calls, year!=2008)

 
library(plyr)
# min.td <- ddply(ia_calls, .(permno, call_date), summarize, 
#                 min.td=min(abs(td)))
# ia_calls <- merge(ia_calls, min.td)
# ia_calls <- subset(ia_calls, min.td==0)

plot.data <- ddply(ia_calls, .(td, quintile_fog_anal_qa), summarize, 
                   fog_anal_qa=mean(fog_anal_qa, na.rm=TRUE),
                   fog_comp_pres=mean(fog_comp_pres, na.rm=TRUE),
                   illiq=median(illiq, na.rm=TRUE),
                   lambda_gh=median(lambda_gh/ewprice, na.rm=TRUE), 
                   spread=median(spread, na.rm=TRUE),
                   n=length(!is.na(ntrades)))
library(ggplot2)
ggplot(data = plot.data, aes(x=td, y=illiq,
                             group=quintile_fog_anal_qa,
                             colour=quintile_fog_anal_qa)) +
  geom_line() + 
  xlab("Trading days from call date") +
  ylab(expression("median ILLIQ")) # [GH]))

ggplot(data = plot.data, aes(x=td, y=spread, group=year, colour=year)) +
  geom_line() + 
  xlab("Trading days from call date") +
  ylab("median spread")

