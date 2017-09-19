library(dplyr, warn.conflicts = FALSE)

library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())

calls <-
    tbl(pg, sql("SELECT * FROM streetevents.calls")) %>%
    collect()
dbDisconnect(pg)

load("data/fog_data_new.Rdata")
load("data/fog_data3.rdata")
fog_data3 %>%
    as_tibble() %>%
    anti_join(fog_data, by="file_name") %>%
    select(file_name) %>%
    anti_join(calls)

fog_data3 %>%
    full_join(fog_data)

old_years <-
    fog_data3 %>%
    filter(fog_comp_pres > 0) %>%
    mutate(year = format(call_date, "%Y")) %>%
    count(year) %>%
    arrange(year)

fog_data %>%
    filter(fog_comp_pres > 0) %>%
    mutate(year = format(start_date, "%Y")) %>%
    count(year) %>%
    full_join(old_years, by="year", suffix=c("_new", "_old")) %>%
    arrange(year)
