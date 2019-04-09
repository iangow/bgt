#!/usr/bin/env Rscript
library(RPostgreSQL)
library(dplyr, warn.conflicts = FALSE)
pg <- dbConnect(PostgreSQL())

calls <- tbl(pg, sql("SELECT * FROM streetevents.calls"))
sics <- tbl(pg, sql("SELECT * FROM bgt.sics"))
long_words <- tbl(pg, sql("SELECT * FROM bgt.long_words"))

dbGetQuery(pg, "SET work_mem='48GB'")

latest_calls <-
    calls %>%
    group_by(file_name) %>%
    summarize(last_update = max(last_update)) %>%
    compute(index=c("file_name"))

word_freq <-
    latest_calls %>%
    inner_join(sics) %>%
    inner_join(long_words) %>%
    group_by(sic2) %>%
    summarize(long_words = count_agg(long_words)) %>%
    compute(name = "word_freq", temporary = FALSE)

dbGetQuery(pg, "DROP TABLE IF EXISTS bgt.word_freq")
dbGetQuery(pg, "ALTER TABLE word_freq OWNER TO bgt")
dbGetQuery(pg, "ALTER TABLE word_freq SET SCHEMA bgt")

pg <- dbDisconnect(pg)
