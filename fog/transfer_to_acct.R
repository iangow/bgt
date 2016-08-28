# Get fog data from database ----
library(dplyr)

pg <- src_postgres()

dbGetQuery(pg$con, "SET work_mem='3GB'")

calls <- tbl(pg, sql("SELECT * FROM streetevents.calls"))

ccmxpf_lnkhist <-
    tbl(pg, sql("SELECT * FROM crsp.ccmxpf_lnkhist"))

crsp_linktable <-
    ccmxpf_lnkhist %>%
    filter(linktype %in% c('LC', 'LU', 'LS')) %>%
    mutate(permno=as.integer(lpermno)) %>%
    select(gvkey, permno, linkdt, linkenddt) %>%
    compute(indexes="permno")

fundq <- tbl(pg, sql("SELECT * FROM comp.fundq"))
secm <- tbl(pg, sql("SELECT * FROM comp.secm"))

fog_recast <- tbl(pg, sql("SELECT * FROM bgt.fog_recast"))

crsp_link <-
    tbl(pg, sql("SELECT * FROM streetevents.crsp_link")) %>%
    select(file_name, permno)

call_dates <-
    calls %>%
    select(file_name, last_update, call_date) %>%
    mutate(call_date=sql("call_date::date")) %>%
    compute()

ticker_match <- tbl(pg, sql("SELECT * FROM streetevents.ticker_match"))

latest_calls <-
    calls %>%
    group_by(file_name) %>%
    summarize(last_update=max(last_update))

# Given a GVKEY and release date, what is the correct PERMNO?
rdqs <-
    fundq %>%
    select(gvkey, rdq) %>%
    distinct()

fog_data <-
    fog_recast %>%
    semi_join(latest_calls) %>%
    inner_join(call_dates) %>%
    inner_join(crsp_link) %>%
    compute()

fog_data_ticker <-
    fog_recast %>%
    semi_join(latest_calls) %>%
    inner_join(call_dates) %>%
    inner_join(ticker_match) %>%
    compute()

# Save data and convert to SAS format ----
if (!dir.exists("data")) dir.create("data")
library(haven)
write_sas(fog_data %>% as.data.frame(), "data/fog_data_new.sas7bdat")
write_sas(fog_data_ticker %>% as.data.frame(), "data/fog_data_ticker_new.sas7bdat")

