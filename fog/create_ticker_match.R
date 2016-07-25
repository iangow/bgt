#
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())
# library(reshape)
library(dplyr)

pg <- src_postgres()

linktable <-
    tbl(pg, sql("SELECT * FROM crsp.ccmxpf_linktable")) %>%
    filter(usedflag==1L, linkprim %in% c('C', 'P')) %>%
    mutate(permno=as.integer(lpermno)) %>%
    select(gvkey, permno, linkdt, linkenddt)

# - Some tickers have *s in them, so I clean them out.
calls <-
    tbl(pg, sql("SELECT * FROM streetevents.calls")) %>%
    mutate(ticker=regexp_replace(ticker, '[*]', '', 'g')) %>%
    mutate(call_date=sql("call_date::date")) %>%
    select(file_name, co_name, call_desc, call_date, last_update, ticker) %>%
    group_by(file_name) %>%
    filter(last_update==max(last_update))

fundq <-
    tbl(pg, sql("SELECT * FROM comp.fundq")) %>%
    mutate(month=eomonth(rdq)) %>%
    select(gvkey, datadate, rdq, conm, month)

secm <-
    tbl(pg, sql("SELECT * FROM comp.secm")) %>%
    mutate(month=eomonth(datadate)) %>%
    select(gvkey, month, tic)

rdqs <-
    fundq %>%
    left_join(secm) %>%
    select(gvkey, tic, rdq, conm, datadate) %>%
    rename(ticker=tic) %>%
    distinct() %>%
    compute()

# Match earnings announcements with calls within three days
# and with the same ticker
ticker_match <-
    calls %>%
    inner_join(rdqs) %>%
    filter(between(call_date, rdq, sql("rdq + interval '3 days'")))

ticker_match %>%
    left_join(linktable) %>%
    filter(datadate >= linkdt, datadate <= linkenddt || is.na(linkenddt)) %>%
    compute(name="ticker_match", indexes="file_name", temporary=FALSE)
rs <-
    RPostgreSQL::dbGetQuery(pg$con, "DROP TABLE IF EXISTS bgt.ticker_match")
rs <- RPostgreSQL::dbGetQuery(pg$con, "ALTER TABLE ticker_match SET SCHEMA bgt")

# Merge tickers from comp.secm with announcement dates
# from comp.fundqrdqs <-
