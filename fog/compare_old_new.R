library(haven)
fog_data_old <- read_sas("data/fog_data.sas7bdat")
fog_data_new <- read_sas("data/fog_data_new.sas7bdat")

fog_sample <-
    read_sas("data/forian.sas7bdat") %>%
    select(file_name)

library(RPostgreSQL)
dbWriteTable(pg$con, c("bgt", "bgt_sample"), fog_sample,
             overwrite=TRUE, row.names=FALSE)

call_years <-
    tbl(pg, sql("SELECT * FROM streetevents.calls")) %>%
    mutate(year=sql("EXTRACT(year FROM call_date)")) %>%
    select(file_name, year) %>%
    collect(n=Inf)

old_names <- colnames(fog_data_old)
old_names <- old_names[!grepl("anal_pres", old_names)]
new_names <- colnames(fog_data_new)

setdiff(old_names, new_names)
setdiff(new_names, old_names)
