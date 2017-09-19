# Get fog data from database ----
library(dplyr, warn.conflicts = FALSE)
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

dbGetQuery(pg, "SET work_mem='3GB'")

ccmxpf_lnkhist <- tbl(pg, sql("SELECT * FROM crsp.ccmxpf_lnkhist"))
crsp_link <- tbl(pg, sql("SELECT * FROM streetevents.crsp_link"))
fundq <- tbl(pg, sql("SELECT * FROM comp.fundq"))
calls <- tbl(pg, sql("SELECT * FROM streetevents.calls"))

crsp_linktable <-
    ccmxpf_lnkhist %>%
    filter(linktype %in% c('LC', 'LU', 'LS')) %>%
    mutate(permno=as.integer(lpermno)) %>%
    select(gvkey, permno, linkdt, linkenddt) %>%
    compute(indexes="permno")

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

se_links <-
    calls %>%
    select(file_name, start_date, last_update) %>%
    group_by(file_name) %>%
    filter(last_update==max(last_update)) %>%
    inner_join(
        crsp_link %>%
        select(file_name, permno) %>%
        distinct()) %>%
    inner_join(rdq_link) %>%
    filter(between(start_date, rdq, sql("rdq + interval '3 days'"))) %>%
    ungroup() %>%
    compute()

load("data/fog_data3.rdata")
fog_data3 <- as_tibble(fog_data3)

### copy_to(pg, fo)

old_links <-
    fog_data3 %>%
    select(file_name, permno) %>%
    distinct()

new_links <-
    se_links %>%
    select(file_name, permno) %>%
    distinct() %>%
    collect()

merged <-
    old_links %>%
    left_join(new_links, by="file_name", suffix=c("_old", "_new"), copy = TRUE) %>%
    mutate(same_same=permno_old==permno_new)

merged %>%
    count(same_same)
