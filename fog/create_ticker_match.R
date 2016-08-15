#
library(dplyr)

pg <- src_postgres()

# Some tickers have *s in them, so I clean them up
calls <- tbl(pg, sql("SELECT * FROM streetevents.calls"))

ccmxpf_lnkhist <-
    tbl(pg, sql("SELECT * FROM crsp.ccmxpf_lnkhist"))

crsp_linktable <-
    ccmxpf_lnkhist %>%
    filter(linktype %in% c('LC', 'LU', 'LS')) %>%
    mutate(permno=as.integer(lpermno)) %>%
    select(gvkey, permno, linkdt, linkenddt) %>%
    compute(indexes="gvkey")

fundq <- tbl(pg, sql("SELECT * FROM comp.fundq"))
secm <- tbl(pg, sql("SELECT * FROM comp.secm"))

tickers <-
    calls %>%
    mutate(call_date=sql("call_date::date")) %>%
    mutate(ticker=sql("regexp_replace(ticker, '[*]', '', 'g')")) %>%
    select(file_name, last_update, ticker, call_date)

secm_tickers <-
    secm %>%
    mutate(eomonth=eomonth(datadate)) %>%
    select(gvkey, eomonth, datadate, tic) %>%
    rename(ticker=tic) %>%
    compute()

rdqs <-
    fundq %>%
    select(gvkey, rdq) %>%
    mutate(eomonth=eomonth(rdq)) %>%
    distinct()

# Merge tickers from comp.secm with announcement dates
# from comp.fundq. Add PERMNOs.
comp_tickers <-
    rdqs %>%
    inner_join(secm_tickers) %>%
    inner_join(crsp_linktable) %>%
    filter(rdq >= linkdt,
           rdq <= linkenddt | is.na(linkenddt)) %>%
    select(ticker, rdq, gvkey, permno) %>%
    compute

# Match earnings announcements with calls within three days
# and with the same ticker
ticker_match <-
    tickers %>%
    inner_join(comp_tickers) %>%
    filter(between(call_date, rdq, sql("rdq + interval '3 days'"))) %>%
    select(file_name, last_update, gvkey, permno) %>%
    compute(name="ticker_match", indexes="file_name", temporary=FALSE)

rs <-
    RPostgreSQL::dbGetQuery(pg$con, "DROP TABLE IF EXISTS streetevents.ticker_match")
rs <- RPostgreSQL::dbGetQuery(pg$con, "ALTER TABLE ticker_match SET SCHEMA streetevents")

