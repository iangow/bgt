#!/usr/bin/env Rscript
library(dplyr)
library(RPostgreSQL)
pg <- src_postgres()

dbGetQuery(pg$con, "SET work_mem='10GB'")

speaker_data <-
    tbl(pg, sql("SELECT * FROM streetevents.speaker_data"))

categories <-
    speaker_data %>%
    filter(context=="qa") %>%
    mutate(category=sql("(CASE WHEN role='Analyst' THEN 'anal' ELSE 'comp' END) || '_qa'")) %>%
    select(file_name, last_update, speaker_number, category) %>%
    group_by(file_name, last_update) %>%
    mutate(median_speaker=(min(speaker_number)+max(speaker_number))/2) %>%
    mutate(first_half=speaker_number <= median_speaker) %>%
    compute() # indexes=c("file_name", "last_update", "speaker_number"))

fog_speaker <-
    tbl(pg, sql("SELECT * FROM bgt.fog_speaker")) %>%
    filter(context=="qa")

rs <- dbGetQuery(pg$con, "DROP TABLE IF EXISTS bgt.fog_by_half")

fog_by_half <-
    fog_speaker %>%
    inner_join(categories) %>%
    group_by(file_name, last_update, category, first_half) %>%
    summarize(fog=0.4*(sum(num_words)/sum(num_sentences) +
                           sum(percent_complex*num_words)/sum(num_words))) %>%
    compute(name="fog_by_half", temporary=FALSE)

RPostgreSQL::dbGetQuery(pg$con, "ALTER TABLE fog_by_half SET SCHEMA bgt")

fog_by_half <- tbl(pg, sql("SELECT * FROM bgt.fog_by_half"))
