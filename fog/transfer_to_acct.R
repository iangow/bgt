# Get fog data from database ----
library(dplyr, warn.conflicts = FALSE)
library(RPostgreSQL)
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
    select(file_name, last_update, start_date) %>%
    mutate(start_date=sql("start_date::date")) %>%
    compute()

latest_calls <-
    calls %>%
    group_by(file_name) %>%
    summarize(last_update=max(last_update))

# Given a GVKEY and release date, what is the correct PERMNO?
rdqs <-
    fundq %>%
    select(gvkey, rdq) %>%
    distinct()

rdq_link <-
    rdqs %>%
    inner_join(crsp_linktable) %>%
    filter(rdq >= linkdt, rdq <= linkenddt | is.na(linkenddt)) %>%
    select(gvkey, permno, rdq) %>%
    compute()

fog_data <-
    fog_recast %>%
    semi_join(latest_calls) %>%
    inner_join(call_dates) %>%
    inner_join(crsp_link) %>%
    inner_join(rdq_link) %>%
    filter(between(start_date, rdq, sql("rdq + interval '3 days'"))) %>%
    compute()

# Save data and convert to SAS format ----
if (!dir.exists("data")) dir.create("data")
library(haven)
fog_data <- fog_data %>% as.data.frame()
save(fog_data, file="data/fog_data_new.Rdata")
system("/Applications/StatTransfer13/st data/fog_data_new.Rdata data/fog_data_new.sas7bdat -y")
