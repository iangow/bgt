library(dplyr)

pg <- src_postgres()

speaker_data <-
    tbl(pg, sql("SELECT * FROM streetevents.speaker_data"))

# RPostgreSQL::dbGetQuery(pg$con, "DROP TABLE IF EXISTS nchars")
RPostgreSQL::dbGetQuery(pg$con, "DROP TABLE IF EXISTS bgt.nchars")

system.time({
    speaker_data %>%

        mutate(nchars = char_length(regexp_replace(speaker_text, "[,.? !]", '')),
               nchars_alt = char_length(regexp_replace(speaker_text, "[^A-Za-z0-9]", ''))) %>%
        select(file_name, last_update, context, speaker_number, nchars, nchars_alt) %>%
        compute(indexes=c("file_name", "last_update", "context", "speaker_number"),
                name="nchars", temporary=FALSE)
})

RPostgreSQL::dbGetQuery(pg$con, "ALTER TABLE nchars SET SCHEMA bgt")

