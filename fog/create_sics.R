#!/usr/bin/env Rscript
library(RPostgreSQL)
library(dplyr, warn.conflicts = FALSE)
pg <- dbConnect(PostgreSQL())

crsp_link <- tbl(pg, sql("SELECT * FROM streetevents.crsp_link"))
calls <- tbl(pg, sql("SELECT * FROM  streetevents.calls"))
stocknames <- tbl(pg, sql("SELECT * FROM crsp.stocknames"))

stocknames %>%
    mutate(sic2 = as.integer(floor(siccd/100))) %>%
    inner_join(crsp_link, by="permno") %>%
    inner_join(calls, by="file_name") %>%
    filter(start_date >= namedt,
           start_date <= nameenddt | is.na(nameenddt)) %>%
    group_by(file_name) %>%
    summarize(sic2 = min(sic2)) %>%
    compute(name="sics", temporary=FALSE, overwrite=TRUE)

dbGetQuery(pg, "DROP TABLE IF EXISTS bgt.sics")
dbGetQuery(pg, "ALTER TABLE sics OWNER TO bgt")
dbGetQuery(pg, "ALTER TABLE sics SET SCHEMA bgt")

dbDisconnect(pg)
