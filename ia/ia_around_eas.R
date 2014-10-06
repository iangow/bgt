library(RPostgreSQL)
drv <- dbDriver("PostgreSQL")
pg <- dbConnect(drv, dbname="crsp")
            
system.time(ia_earnannc <- dbGetQuery(pg, "
  SET work_mem='16GB';
  
  WITH  
  linktable AS (
    SELECT gvkey, lpermno::integer AS permno, linkdt, linkenddt
    FROM crsp.ccmxpf_linktable 
    WHERE usedflag=1 AND linkprim IN ('C', 'P')),
  merged_fog_data AS (
    SELECT DISTINCT a.fdate, a.read_fog AS fog_earnannc, b.permno,  
      ntile(5) over (
        PARTITION BY extract(year FROM fdate) 
        ORDER BY read_fog) AS quintile_fog_earnannc
    FROM bgt.measures_earnannc AS a
    LEFT JOIN linktable AS b
    ON a.gvkey=b.gvkey AND 
      a.fdate >= b.linkdt AND
      (a.fdate <= b.linkenddt OR b.linkenddt IS NULL)),
  fog_data_w_td AS (
    SELECT DISTINCT a.*, b.td 
    FROM merged_fog_data AS a
    INNER JOIN crsp.anncdates AS b
    ON a.fdate=b.anncdate),
    SELECT a.permno, extract(year FROM a.date) AS year, a.date, a.lambda_gh, a.ntrades, 
      twspread/twprice AS spread, c.td - b.td AS td, 
      b.permno, b.quintile_fog_earnannc, b.fog_earnannc
    FROM bgt.ia_daily AS a
    INNER JOIN crsp.anncdates AS c
    ON a.date=c.anncdate
    INNER JOIN fog_data_w_td AS b
    ON a.permno=b.permno AND c.td - b.td BETWEEN - 30 AND 30"))

system.time(ia_mda <- dbGetQuery(pg, "
  SET work_mem='16GB';
  
  WITH  
  linktable AS (
    SELECT gvkey, lpermno::integer AS permno, linkdt, linkenddt
    FROM crsp.ccmxpf_linktable 
    WHERE usedflag=1 AND linkprim IN ('C', 'P')),
  merged_fog_data AS (
    SELECT DISTINCT a.fdate, a.read_fog AS fog_mda, b.permno,  
      ntile(5) over (
        PARTITION BY extract(year FROM fdate) 
        ORDER BY read_fog) AS quintile_fog_mda
    FROM bgt.measures_mda AS a
    LEFT JOIN linktable AS b
    ON a.gvkey=b.gvkey AND 
      a.fdate >= b.linkdt AND
      (a.fdate <= b.linkenddt OR b.linkenddt IS NULL)),
  fog_data_w_td AS (
    SELECT DISTINCT a.*, b.td 
    FROM merged_fog_data AS a
    INNER JOIN crsp.anncdates AS b
    ON a.fdate=b.anncdate)
  SELECT a.permno, extract(year FROM a.date) AS year, a.date, a.lambda_gh, a.ntrades, 
    twspread/twprice AS spread, c.td - b.td AS td, 
    b.permno, b.quintile_fog_mda, b.fog_mda
  FROM bgt.ia_daily AS a
  INNER JOIN crsp.anncdates AS c
  ON a.date=c.anncdate
  INNER JOIN fog_data_w_td AS b
  ON a.permno=b.permno AND c.td - b.td BETWEEN -30 AND 30
"))

ia_earnannc$quintile_fog_earnannc <- as.factor(ia_earnannc$quintile_fog_earnannc)
ia_mda$quintile_fog_mda <- as.factor(ia_mda$quintile_fog_mda)

# ia_earnannc[, dim(ia_earnannc)[2]] <- NULL
library(plyr)
plot.data.ea <- ddply(ia_earnannc, .(td, quintile_fog_earnannc), summarize, 
                      fog_earnannc=mean(fog_earnannc, na.rm=TRUE),
                      lambda_gh=median(lambda_gh, na.rm=TRUE), 
                      spread=median(spread, na.rm=TRUE),
                      n=length(!is.na(ntrades)))

plot.data.mda <- ddply(ia_mda, .(td, quintile_fog_mda), summarize, 
                      fog_mda=mean(fog_mda, na.rm=TRUE),
                      lambda_gh=median(lambda_gh, na.rm=TRUE), 
                      spread=median(spread, na.rm=TRUE),
                      n=length(!is.na(ntrades)))


pdf("~/Dropbox/research/BuGT/drafts/ia_plots.pdf", width=9, paper="USr")

library(ggplot2)
ggplot(data = plot.data.ea, aes(x=td, y=lambda_gh,
                             group=quintile_fog_earnannc,
                             colour=quintile_fog_earnannc)) +
  geom_line() + 
  labs(title="Information asymmetry around earnings announcements") +
  xlab("Trading days from earnings announcement date") +
  ylab(expression("Median " * lambda[GH]))

ggplot(data = plot.data.ea, aes(x=td, y=spread, 
                             group=quintile_fog_earnannc,
                             colour=quintile_fog_earnannc)) +
  geom_line() + 
  labs(title="Information asymmetry around earnings announcements") +
  xlab("Trading days from earnings announcement date") +
  ylab("Median spread")

ggplot(data = plot.data.mda, aes(x=td, y=lambda_gh,
                                group=quintile_fog_mda,
                                colour=quintile_fog_mda)) +
  geom_line() + 
  labs(title="Information asymmetry around 10-K/10-Q filing dates") +
  xlab("Trading days from earnings 10-K/10-Q filing date") +
  ylab(expression("Median " * lambda[GH]))

ggplot(data= plot.data.mda, aes(x=td, y=spread, 
                                group=quintile_fog_mda,
                                colour=quintile_fog_mda)) +
  geom_line() + 
  labs(title="Information asymmetry around 10-K/10-Q filing dates") +
  xlab("Trading days from earnings 10-K/10-Q filing date") +
  ylab("Median spread")

dev.off()
