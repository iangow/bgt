library(dplyr, warn.conflicts = FALSE)

library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

calls <-
    tbl(pg, sql("SELECT * FROM streetevents.calls")) %>%
    select(file_name) %>%
    distinct() %>%
    collect()
dbDisconnect(pg)

load("data/fog_data_new.Rdata")
load("data/fog_data3.rdata")
fog_data3 <- as_tibble(fog_data3)
fog_data_new <- as_tibble(fog_data_save)
rm(fog_data_save)

missing_calls <-
    fog_data3 %>%
    filter(!is.na(rdq)) %>%
    anti_join(fog_data_new, by="file_name") %>%
    inner_join(calls) %>%
    select(file_name, call_date)

missing_calls %>%
    mutate(year = format(call_date, "%Y")) %>%
    count(year) %>%
    arrange(year)

fog_data3 %>%
    anti_join(calls) %>%
    mutate(year = format(call_date, "%Y")) %>%
    count(year) %>%
    arrange(year)

old_years <-
    fog_data3 %>%
    filter(!is.na(gvkey)) %>%
    filter(fog_comp_pres > 0) %>%
    mutate(year = format(call_date, "%Y")) %>%
    count(year) %>%
    arrange(year)

new_years <-
    fog_data_new %>%
    filter(!is.na(gvkey)) %>%
    filter(fog_comp_pres > 0) %>%
    mutate(year = format(start_date, "%Y")) %>%
    count(year)

combined <-
    new_years %>%
    full_join(old_years, by="year", suffix=c("_new", "_old")) %>%
    arrange(year)

combined
