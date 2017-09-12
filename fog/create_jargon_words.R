library(RPostgreSQL)
library(dplyr, warn.conflicts = FALSE)
pg <- dbConnect(PostgreSQL())

word_freq <- tbl(pg, sql("SELECT * FROM bgt.word_freq"))
sics <- tbl(pg, sql("SELECT * FROM bgt.sics"))
long_words <- tbl(pg, sql("SELECT * FROM bgt.long_words"))

top_words <-
    word_freq %>%
    mutate(top_words = top_words(long_words, 100L)) %>%
    select(-long_words)

dbGetQuery(pg, "SET work_mem='18GB'")

top_words %>%
    inner_join(sics) %>%
    inner_join(long_words) %>%
    mutate(overlap = array_overlap(long_words, top_words)) %>%
    mutate(num_jargon_words = array_length(overlap, 1L)) %>%
    select(file_name,  last_update, category, num_jargon_words) %>%
    compute(name="jargon_words", temporary=FALSE,
            indexes=list(c("file_name", "category")))

dbGetQuery(pg, "DROP TABLE IF EXISTS bgt.jargon_words")
dbGetQuery(pg, "ALTER TABLE jargon_words OWNER TO bgt")
dbGetQuery(pg, "ALTER TABLE jargon_words SET SCHEMA bgt")
