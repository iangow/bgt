#!/usr/bin/env Rscript
# Get fog data from database ----
library(dplyr, warn.conflicts = FALSE)
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

dbGetQuery(pg, "SET work_mem='3GB'")

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

fog_recast <- tbl(pg, sql("SELECT * FROM bgt.fog_recast"))

crsp_link <-
    tbl(pg, sql("SELECT * FROM streetevents.crsp_link")) %>%
    select(file_name, permno)

call_dates <-
    calls %>%
    select(file_name, last_update, start_date) %>%
    mutate(call_date=sql("(start_date AT TIME ZONE 'America/New_York')::date")) %>%
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
    filter(rdq >= linkdt | is.na(linkdt), rdq <= linkenddt | is.na(linkenddt)) %>%
    select(gvkey, permno, rdq) %>%
    compute()

fog_data <-
    fog_recast %>%
    semi_join(latest_calls) %>%
    inner_join(call_dates) %>%
    inner_join(crsp_link) %>%
    inner_join(rdq_link) %>%
    filter(between(start_date, sql("rdq - interval '1 day'"), sql("rdq + interval '3 days'"))) %>%
    compute(name="fog_data", temporary=FALSE)

fog_data_save <-
    fog_data %>%
    collect() %>%
    as.data.frame()

dbGetQuery(pg, "DROP TABLE IF EXISTS bgt.fog_data")
dbGetQuery(pg, "ALTER TABLE fog_data OWNER TO bgt")
dbGetQuery(pg, "ALTER TABLE fog_data SET SCHEMA bgt")

# Save data and convert to SAS format ----
if (!dir.exists("data")) dir.create("data")
library(haven)
write_dta(fog_data_save, path="data/fog_data_new.dta", version = 12)

library(readr)
write_csv(fog_data_save, path="data/fog_data_new.csv")
rm(fog_data_save)
